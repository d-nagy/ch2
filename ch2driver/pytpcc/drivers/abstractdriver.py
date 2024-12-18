# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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

from datetime import datetime

import constants

## ==============================================
## AbstractDriver
## ==============================================
class AbstractDriver(object):
    def __init__(self, name, ddl):
        self.name = name
        self.driver_name = "%sDriver" % self.name.title()
        self.ddl = ddl
        
    def __str__(self):
        return self.driver_name
    
    def makeDefaultConfig(self):
        """This function needs to be implemented by all sub-classes.
        It should return the items that need to be in your implementation's configuration file.
        Each item in the list is a triplet containing: ( <PARAMETER NAME>, <DESCRIPTION>, <DEFAULT VALUE> )
        """
        raise NotImplementedError("%s does not implement makeDefaultConfig" % (self.driver_name))
    
    def loadConfig(self, config):
        """Initialize the driver using the given configuration dict"""
        raise NotImplementedError("%s does not implement loadConfig" % (self.driver_name))
        
    def formatConfig(self, config):
        """Return a formatted version of the config dict that can be used with the --config command line argument"""
        ret =  "# %s Configuration File\n" % (self.driver_name)
        ret += "# Created %s\n" % (datetime.now())
        ret += "[%s]" % self.name
        
        for name in config.keys():
            desc, default = config[name]
            if default == None: default = ""
            ret += "\n\n# %s\n%-20s = %s" % (desc, name, default) 
        return (ret)
        
    def getOneDoc(self, tableName, fieldValues, generateKey=False):
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            return self.getOneCH2Doc(tableName, fieldValues, generateKey)
        elif self.schema == constants.CH2_DRIVER_SCHEMA["CH2PPF"]:
            return self.getOneCH2PPFlatDoc(tableName, fieldValues, generateKey)
        else:
            return self.getOneCH2PPDoc(tableName, fieldValues, generateKey)

    def getOneCH2Doc(self, tableName, fieldValues, generateKey):
        columns = constants.CH2_TABLE_COLUMNS[tableName]
        key = ""
        if generateKey:
            key = ".".join(str(fieldValues[k]) for k in constants.KEYNAMES[tableName])
        val = {}
        for l, v in enumerate(fieldValues):
            v1 = fieldValues[l]
            if tableName == constants.TABLENAME_ORDERS and columns[l] == "o_orderline":
                v1 = []
                for olv in v:
                    v1.append(self.genDoc(olv, constants.TABLENAME_ORDERLINE))
            elif (tableName == constants.TABLENAME_ITEM and columns[l] == "i_categories" or
                  tableName == constants.TABLENAME_CUSTOMER and columns[l] == "c_item_categories"):
                continue
            elif tableName == constants.TABLENAME_CUSTOMER and columns[l] == "c_extra":
                for i in range(0, self.customerExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ORDERS and columns[l] == "o_extra":
                for i in range(0, self.ordersExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and columns[l] == "i_extra":
                for i in range(0, self.itemExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif isinstance(v1,(datetime)):
                v1 = str(v1)
            val[columns[l]] = v1

        return key, val

    def getOneCH2PPDoc(self, tableName, fieldValues, generateKey):
        columns = constants.CH2PP_TABLE_COLUMNS[tableName]
        key = ""
        if generateKey:
            key = ".".join(str(fieldValues[k]) for k in constants.KEYNAMES[tableName])
        val = {}
        for l, v in enumerate(fieldValues):
            v1 = fieldValues[l]
            if isinstance(v1,(datetime)):
                v1 = str(v1)
            elif tableName == constants.TABLENAME_ORDERS and columns[l] == "o_orderline":
                v1 = []
                for olv in v:
                    v1.append(self.genDoc(olv, constants.TABLENAME_ORDERLINE))
            elif (self.schema == constants.CH2_DRIVER_SCHEMA["CH2P"] and
                  (tableName == constants.TABLENAME_ITEM and columns[l] == "i_categories" or
                   tableName == constants.TABLENAME_CUSTOMER and columns[l] == "c_item_categories")):
                continue
            elif tableName == constants.TABLENAME_WAREHOUSE and columns[l] == "w_address":
                v1 = self.genDoc(v, constants.TABLENAME_WAREHOUSE_ADDRESS)
            elif tableName == constants.TABLENAME_DISTRICT and columns[l] == "d_address":
                v1 = self.genDoc(v, constants.TABLENAME_DISTRICT_ADDRESS)
            elif tableName == constants.TABLENAME_SUPPLIER and columns[l] == "su_address":
                v1 = self.genDoc(v, constants.TABLENAME_SUPPLIER_ADDRESS)
            elif tableName == constants.TABLENAME_CUSTOMER:
                if columns[l] == "c_name":
                    v1 = self.genDoc(v, constants.TABLENAME_CUSTOMER_NAME)
                elif columns[l] == "c_extra":
                    for i in range(0, self.customerExtraFields):
                        val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                    continue
                elif columns[l] == "c_addresses":
                    v1 = []
                    for clv in v:
                        v1.append(self.genDoc(clv, constants.TABLENAME_CUSTOMER_ADDRESSES))
                        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2P"]:
                            break # Load only one customer address for CH2P
                elif columns[l] == "c_phones":
                    v1 = []
                    for clv in v:
                        v1.append(self.genDoc(clv, constants.TABLENAME_CUSTOMER_PHONES))
                        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2P"]:
                            break # Load only one customer phone for CH2P
            elif tableName == constants.TABLENAME_ORDERS and columns[l] == "o_extra":
                for i in range(0, self.ordersExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and columns[l] == "i_extra":
                for i in range(0, self.itemExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            val[columns[l]] = v1
        return key, val

    def getOneCH2PPFlatDoc(self, tableName, fieldValues, generateKey):
        columns = constants.CH2PP_TABLE_COLUMNS[tableName]
        key = ""
        if generateKey:
            key = ".".join(str(fieldValues[k]) for k in constants.KEYNAMES[tableName])
        val = {}
        for l, v in enumerate(fieldValues):
            v1 = fieldValues[l]
            if isinstance(v1,(datetime)):
                v1 = str(v1)
            elif tableName == constants.TABLENAME_ORDERS and columns[l] == "o_orderline":
                continue
            elif tableName == constants.TABLENAME_WAREHOUSE and columns[l] == "w_address":
                self.addFlatFields(v1, constants.TABLENAME_WAREHOUSE_ADDRESS, val)
                continue
            elif tableName == constants.TABLENAME_DISTRICT and columns[l] == "d_address":
                self.addFlatFields(v1, constants.TABLENAME_DISTRICT_ADDRESS, val)
                continue
            elif tableName == constants.TABLENAME_SUPPLIER and columns[l] == "su_address":
                self.addFlatFields(v1, constants.TABLENAME_SUPPLIER_ADDRESS, val)
                continue
            elif tableName == constants.TABLENAME_CUSTOMER:
                if columns[l] == "c_name":
                    self.addFlatFields(v1, constants.TABLENAME_CUSTOMER_NAME, val)
                    continue
                elif columns[l] == "c_extra":
                    for i in range(0, self.customerExtraFields):
                        val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                    continue
                elif columns[l] == "c_addresses" or columns[l] == "c_phones" or columns[l] == "c_item_categories":
                    continue
            elif tableName == constants.TABLENAME_ORDERS and columns[l] == "o_extra":
                for i in range(0, self.ordersExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and columns[l] == "i_extra":
                for i in range(0, self.itemExtraFields):
                    val[columns[l]+"_"+str(format(i+1, "03d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_STOCK and columns[l] == "s_dists":
                for i in range(0, 10):
                    val[columns[l][:-1]+"_"+str(format(i+1, "02d"))] = v1[i]
                continue
            elif tableName == constants.TABLENAME_ITEM and columns[l] == "i_categories":
                  continue
            val[columns[l]] = v1
        return key, val

    def genDoc(self, fieldValues, tableName, rval=None):
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            columns = constants.CH2_TABLE_COLUMNS[tableName]
        else:
            columns = constants.CH2PP_TABLE_COLUMNS[tableName]
        rval = rval or {}
        for l, v in enumerate(fieldValues):
            if isinstance(v,(datetime)):
                v = str(v)
            rval[columns[l]] = v
        return rval

    def addFlatFields(self, v1, tableName, rval):
        return self.genDoc(v1, tableName, rval)

    def loadStart(self):
        """Optional callback to indicate to the driver that the data loading phase is about to begin."""
        return None
        
    def loadFinish(self):
        """Optional callback to indicate to the driver that the data loading phase is finished."""
        return None

    def loadFinishItem(self):
        """Optional callback to indicate to the driver that the ITEM data has been passed to the driver."""
        return None

    def loadFinishWarehouse(self, w_id):
        """Optional callback to indicate to the driver that the data for the given warehouse is finished."""
        return None
        
    def loadFinishDistrict(self, w_id, d_id):
        """Optional callback to indicate to the driver that the data for the given district is finished."""
        return None
        
    def loadTuples(self, tableName, tuples):
        """Load a list of tuples into the target table"""
        raise NotImplementedError("%s does not implement loadTuples" % (self.driver_name))
        
    def executeStart(self):
        """Optional callback before the execution phase starts"""
        return None
        
    def executeFinish(self):
        """Callback after the execution phase finishes"""
        return None
        
    def executeTransaction(self, txn, params, duration, endBenchmarkTime, queryIterNum):
        """Execute a transaction based on the given name"""
        if constants.TransactionTypes.DELIVERY == txn:
            result = self.doDelivery(params)
        elif constants.TransactionTypes.NEW_ORDER == txn:
            result = self.doNewOrder(params)
        elif constants.TransactionTypes.ORDER_STATUS == txn:
            result = self.doOrderStatus(params)
        elif constants.TransactionTypes.PAYMENT == txn:
            result = self.doPayment(params)
        elif constants.TransactionTypes.STOCK_LEVEL == txn:
            result = self.doStockLevel(params)
        elif constants.QueryTypes.CH2 == txn:
            result = self.runCH2Queries(duration, endBenchmarkTime, queryIterNum)
        else:
            assert False, "Unexpected TransactionType: " + txn
        return result, self.txStatus()
        
    def doDelivery(self, params):
        """Execute DELIVERY Transaction
        Parameters Dict:
            w_id
            o_carrier_id
            ol_delivery_d
        """
        raise NotImplementedError("%s does not implement doDelivery" % (self.driver_name))
    
    def doNewOrder(self, params):
        """Execute NEW_ORDER Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            o_entry_d
            i_ids
            i_w_ids
            i_qtys
        """
        raise NotImplementedError("%s does not implement doNewOrder" % (self.driver_name))

    def doOrderStatus(self, params):
        """Execute ORDER_STATUS Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            c_last
        """
        raise NotImplementedError("%s does not implement doOrderStatus" % (self.driver_name))

    def doPayment(self, params):
        """Execute PAYMENT Transaction
        Parameters Dict:
            w_id
            d_id
            h_amount
            c_w_id
            c_d_id
            c_id
            c_last
            h_date
        """
        raise NotImplementedError("%s does not implement doPayment" % (self.driver_name))

    def doStockLevel(self, params):
        """Execute STOCK_LEVEL Transaction
        Parameters Dict:
            w_id
            d_id
            threshold
        """
        raise NotImplementedError("%s does not implement doStockLevel" % (self.driver_name))
## CLASS
