# / -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import logging
from pathlib import Path

import orjson

import constants
from .abstractdriver import AbstractDriver


from .nestcollectionsdriver import KEYNAMES, CH2_TABLE_COLUMNS, CH2PP_TABLE_COLUMNS

from datetime import datetime


## ==============================================
## NestcollectionsDocGenDriver
## ==============================================
class NestcollectionsdocgenDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "output_dir": (
            "The path to the directory to store the JSON files",
            "/tmp/tpcc-tables",
        ),
    }

    def __init__(
        self,
        ddl,
        clientId,
        TAFlag="L",
        schema=constants.CH2_DRIVER_SCHEMA["CH2"],
        preparedTransactionQueries={},
        analyticalQueries=constants.CH2_DRIVER_ANALYTICAL_QUERIES[
            "HAND_OPTIMIZED_QUERIES"
        ],
        customerExtraFields=constants.CH2_CUSTOMER_EXTRA_FIELDS["NOT_SET"],
        ordersExtraFields=constants.CH2_ORDERS_EXTRA_FIELDS["NOT_SET"],
        itemExtraFields=constants.CH2_ITEM_EXTRA_FIELDS["NOT_SET"],
        load_mode=constants.CH2_DRIVER_LOAD_MODE["NOT_SET"],
        kv_timeout=constants.CH2_DRIVER_KV_TIMEOUT,
        bulkload_batch_size=constants.CH2_DRIVER_BULKLOAD_BATCH_SIZE,
    ):
        super().__init__("nestcollectionsdocgen", ddl)
        self.client_id = clientId
        self.schema = schema
        self.bulkload_batch_size = bulkload_batch_size
        self.denormalize = False

        self.customerExtraFields = customerExtraFields
        self.ordersExtraFields = ordersExtraFields
        self.itemExtraFields = itemExtraFields

        self.TABLE_COLUMNS = (
            CH2_TABLE_COLUMNS
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]
            else CH2PP_TABLE_COLUMNS
        )
        self.OL_COLUMNS = (
            CH2_TABLE_COLUMNS[constants.TABLENAME_ORDERLINE]
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]
            else CH2PP_TABLE_COLUMNS[constants.TABLENAME_ORDERLINE]
        )

        self.batches = {tableName: [0, [], 0] for tableName in self.TABLE_COLUMNS}

    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return self.DEFAULT_CONFIG

    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        self.output_dir = Path(config["output_dir"])
        assert self.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return

    def saveTuples(self, tableName: str, tuples: list[dict], batch_idx: int) -> bool:
        filename = self.output_dir / (
            "%s-%d-%d.json"
            % (
                tableName,
                self.client_id,
                batch_idx,
            )
        )
        try:
            with open(filename, "wb") as f:
                f.writelines(tuples)
        except Exception as e:
            logging.error("Error saving tuples to file: %s" % str(e))
            return False

        return True

    ## ----------------------------------------------
    ## loadTuples for Couchbase (Adapted from MongoDB implemenetation).
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0:
            return

        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))
        assert tableName in self.TABLE_COLUMNS, "Unexpected table %s" % tableName

        batch_idx, cur_batch, batch_size = self.batches[tableName]
        # For bulk load: load in batches
        for t in tuples:
            val = orjson.dumps(
                self.getOneDoc(tableName, t), option=orjson.OPT_APPEND_NEWLINE
            )
            cur_batch.append(val)
            batch_size += len(val)
            if batch_size > self.bulkload_batch_size:
                result = self.saveTuples(tableName, cur_batch, batch_idx)
                if result:
                    batch_idx += 1
                    cur_batch = []
                    batch_size = 0
                    continue
                else:
                    logging.debug(
                        "Client ID # %d failed bulk load data into KV, aborting..."
                        % self.client_id
                    )

        self.batches[tableName] = [batch_idx, cur_batch, batch_size]

    def getOneDoc(self, tableName, tuple):
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            return self.getOneCH2Doc(tableName, tuple)

        return self.getOneCH2PPDoc(tableName, tuple)

    def getOneCH2Doc(self, tableName, tuple):
        columns = self.TABLE_COLUMNS[tableName]
        keynames = KEYNAMES[tableName]

        key = str(tuple[keynames[0]])
        for k in keynames[1:]:
            key += ".%s" % tuple[k]

        val = {"key": key}
        for v, col in zip(tuple, columns):
            v1 = v
            if tableName == constants.TABLENAME_ORDERS and col == "o_orderline":
                v1 = [
                    self.genNestedTuple(olv, constants.TABLENAME_ORDERLINE) for olv in v
                ]
            elif (
                tableName == constants.TABLENAME_ITEM
                and col == "i_categories"
                or tableName == constants.TABLENAME_CUSTOMER
                and col == "c_item_categories"
            ):
                continue
            elif tableName == constants.TABLENAME_CUSTOMER and col == "c_extra":
                for i in range(self.customerExtraFields):
                    val[f"{col}_{i+1:03d}"] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ORDERS and col == "o_extra":
                for i in range(self.ordersExtraFields):
                    val[f"{col}_{i+1:03d}"] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and col == "i_extra":
                for i in range(self.itemExtraFields):
                    val[f"{col}_{i+1:03d}"] = v1[i]
                continue
            elif isinstance(v1, datetime):
                v1 = str(v1)
            val[col] = v1

        return val

    def getOneCH2PPDoc(self, tableName, tuple):
        columns = self.TABLE_COLUMNS[tableName]
        keynames = KEYNAMES[tableName]

        # load only one customer address for CH2P
        address_slice = (
            slice(1)
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]
            else slice(None)
        )
        # load only one customer phone for CH2P
        phone_slice = (
            slice(1)
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]
            else slice(None)
        )

        key = str(tuple[keynames[0]])
        for k in keynames[1:]:
            key += ".%s" % tuple[k]

        val = {"key": key}
        for v, col in zip(tuple, columns):
            v1 = v
            if tableName == constants.TABLENAME_ORDERS and col == "o_orderline":
                v1 = [
                    self.genNestedTuple(olv, constants.TABLENAME_ORDERLINE) for olv in v
                ]
            elif self.schema == constants.CH2_DRIVER_SCHEMA["CH2P"] and (
                tableName == constants.TABLENAME_ITEM
                and col == "i_categories"
                or tableName == constants.TABLENAME_CUSTOMER
                and col == "c_item_categories"
            ):
                continue
            elif tableName == constants.TABLENAME_WAREHOUSE and col == "w_address":
                v1 = self.genNestedTuple(v, constants.TABLENAME_WAREHOUSE_ADDRESS)
            elif tableName == constants.TABLENAME_DISTRICT and col == "d_address":
                v1 = self.genNestedTuple(v, constants.TABLENAME_DISTRICT_ADDRESS)
            elif tableName == constants.TABLENAME_SUPPLIER and col == "su_address":
                v1 = self.genNestedTuple(v, constants.TABLENAME_SUPPLIER_ADDRESS)
            elif tableName == constants.TABLENAME_CUSTOMER:
                if col == "c_name":
                    v1 = self.genNestedTuple(v, constants.TABLENAME_CUSTOMER_NAME)
                elif col == "c_extra":
                    for i in range(self.customerExtraFields):
                        val[f"{col}_{i+1:03d}"] = v1[i]
                    continue
                elif col == "c_addresses":
                    v1 = [
                        self.genNestedTuple(clv, constants.TABLENAME_CUSTOMER_ADDRESSES)
                        for clv in v[address_slice]
                    ]
                elif col == "c_phones":
                    v1 = [
                        self.genNestedTuple(clv, constants.TABLENAME_CUSTOMER_PHONES)
                        for clv in v[phone_slice]
                    ]
            elif tableName == constants.TABLENAME_ORDERS and col == "o_extra":
                for i in range(self.ordersExtraFields):
                    val[f"{col}_{i+1:03d}"] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and col == "i_extra":
                for i in range(self.itemExtraFields):
                    val[f"{col}_{i+1:03d}"] = v1[i]
                continue
            elif isinstance(v1, datetime):
                v1 = str(v1)
            val[col] = v1

        return val

    def genNestedTuple(self, tuple, tableName):
        rval = {
            col: str(v) if isinstance(v, datetime) else v
            for col, v in zip(self.TABLE_COLUMNS[tableName], tuple)
        }
        return rval

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Client ID # %d Writing last batches to disk" % (self.client_id))
        for tableName, (batch_idx, cur_batch, _) in self.batches.items():
            if cur_batch:
                self.saveTuples(tableName, cur_batch, batch_idx)

        logging.info("Client ID # %d Finished loading tables" % (self.client_id))
        return


## CLASS
