# -*- coding: utf-8 -*-
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

import random
import string

import numpy as np
from . import nurand

SYLLABLES = [ "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" ]

nurandVar = None # NURand
def setNURand(nu):
    global nurandVar
    nurandVar = nu
## DEF

def NURand(a, x, y):
    """A non-uniform random number, as defined by TPC-C 2.1.6. (page 20)."""
    global nurandVar
    assert x <= y
#    assert nurand != None
    if nurandVar is None:
        setNURand(nurand.makeForLoad())
    if a == 255:
        c = nurandVar.cLast
    elif a == 1023:
        c = nurandVar.cId
    elif a == 8191:
        c = nurandVar.orderLineItemId
    else:
        raise Exception("a = " + a + " is not a supported value")
    
    return (((number(0, a) | number(x, y)) + c) % (y - x + 1)) + x
## DEF

def number(minimum, maximum):
    value = int(random.random() * (maximum - minimum + 1)) + minimum
    assert minimum <= value and value <= maximum
    return value
## DEF

def numberExcluding(minimum, maximum, excluding):
    """An in the range [minimum, maximum], excluding excluding."""
    assert minimum < maximum
    assert minimum <= excluding and excluding <= maximum

    ## Generate 1 less number than the range
    num = number(minimum, maximum-1)

    ## Adjust the numbers to remove excluding
    if num >= excluding: num += 1
    assert minimum <= num and num <= maximum and num != excluding
    return num
## DEF 

def fixedPoint(decimal_places, minimum, maximum):
    assert decimal_places > 0
    assert minimum < maximum

    multiplier = 10**decimal_places
    int_min = int(minimum * multiplier + 0.5)
    int_max = int(maximum * multiplier + 0.5)

    return float(number(int_min, int_max) / float(multiplier))
## DEF

def selectUniqueIds(numUnique, minimum, maximum):
    rows = set()
    for i in range(0, numUnique):
        index = None
        while index == None or index in rows:
            index = number(minimum, maximum)
        ## WHILE
        rows.add(index)
    ## FOR
    assert len(rows) == numUnique
    return rows
## DEF


def _gen_random_bytes_for_astring():
    # ord('a') = 97, ord('z') = 122
    return np.random.randint(97, 123, size=10_000_000, dtype="int32").view("U1")


random_bytes_for_astring = _gen_random_bytes_for_astring()
astring_randint_idx = 0


def astring(minimum_length, maximum_length):
    """A random alphabetic string with length in range [minimum_length, maximum_length]."""
    global random_bytes_for_astring
    global astring_randint_idx
    length = number(minimum_length, maximum_length)

    if astring_randint_idx + length >= random_bytes_for_astring.size:
        random_bytes_for_astring = _gen_random_bytes_for_astring()
        astring_randint_idx = 0

    string = (
        random_bytes_for_astring[astring_randint_idx : astring_randint_idx + length]
        .view("U%d" % length)
        .item()
    )
    astring_randint_idx += length
    return string


## DEF

def _gen_random_bytes_for_nstring():
    # ord('0') = 48, ord('9') = 57
    return np.random.randint(48, 58, size=10_000_000, dtype="int32").view("U1")


random_bytes_for_nstring = _gen_random_bytes_for_nstring()
nstring_randint_idx = 0


def nstring(minimum_length, maximum_length):
    """A random numeric string with length in range [minimum_length, maximum_length]."""
    global random_bytes_for_nstring
    global nstring_randint_idx
    length = number(minimum_length, maximum_length)

    if nstring_randint_idx + length >= random_bytes_for_nstring.size:
        random_bytes_for_nstring = _gen_random_bytes_for_nstring()
        nstring_randint_idx = 0

    string = (
        random_bytes_for_nstring[nstring_randint_idx : nstring_randint_idx + length]
        .view("U%d" % length)
        .item()
    )
    nstring_randint_idx += length
    return string


## DEF

def randomStringMinMax(minimum_length, maximum_length):
    length = number(minimum_length, maximum_length)
    return randomStringLength(length)


## DEF

alphanumeric = np.array(list(string.ascii_letters + string.digits))


def randomStringLength(length):
   # With combination of lower and upper case and digits
    return np.random.choice(alphanumeric, length).view("U%d" % length).item()


## DEF

def randomStringsWithEmbeddedSubstrings(minimum_length, maximum_length, substr1, substr2):
    lenSubstr1 = len(substr1)
    lenSubstr2 = len(substr2)
    rlength = 0
    while rlength < lenSubstr1 + lenSubstr2:
        rlength = number(minimum_length, maximum_length)
    l1 = number(0, rlength - lenSubstr1 - lenSubstr2)
    l2 = number(0, rlength - l1 - lenSubstr1 - lenSubstr2)
    l3 = rlength - l1 - l2 - lenSubstr1 - lenSubstr2
    return (
        randomStringLength(l1)
        if l1
        else "" + substr1 + randomStringLength(l2)
        if l2
        else "" + substr2 + randomStringLength(l3)
    )


## DEF

def makeLastName(number):
    """A last name as defined by TPC-C 4.3.2.3. Not actually random."""
    global SYLLABLES
    assert 0 <= number and number <= 999
    indicies = [ int(number/100), int((number/10)%10), int(number%10) ]
    return "".join(map(lambda x: SYLLABLES[x], indicies))
## DEF

def makeRandomLastName(maxCID):
    """A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to maxCID."""
    min_cid = 999
    if (maxCID - 1) < min_cid: min_cid = maxCID - 1
    return makeLastName(NURand(255, 0, min_cid))
## DEF
