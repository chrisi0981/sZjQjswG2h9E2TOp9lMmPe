#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import math
import array
import sys
from multiprocessing import Process
from multiprocessing import Pool
import os
import thread
import threading
import Queue
import numpy
import random
from scipy.optimize import fsolve
import Database_Handler

from operator import itemgetter
import pandas as panda

import time

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "root"
PASSWORD = "htvgt794jj"

# Entropy calculation using the Lempel-Ziv estimation
def containsQuery(history,query):
    n = len(query)    
    return any( (query == history[i:i+n]) for i in xrange(len(history)-n+1) )

def checkQuery(history,current):
    for i in range(0,len(current)+1):
        if not containsQuery(history,current[0:i]):
            return i
        
    return 0

def randomizeData(originalData,fraction):    
    
    data = []
    data_1 = []
    
    for index in range(0,len(originalData)):
        data.append(int(originalData[index]))
        data_1.append(int(originalData[index]))
    
    unknowns = int(len(data)*fraction) # number of unknown locations
    
    upi = int(unknowns/5) # unkownsPerInterval
    ipi = int(len(data)/5) # indiciesPerInterval
    
    indicies_1 = random.sample(range(0,ipi),upi)
    indicies_2 = random.sample(range(ipi,2*ipi),upi)
    indicies_3 = random.sample(range(2*ipi,3*ipi),upi)
    indicies_4 = random.sample(range(3*ipi,4*ipi),upi)
    indicies_5 = random.sample(range(4*ipi,len(data)),upi)
    
    for i in range(0,upi):
        data[indicies_1[i]] = 17
        data[indicies_2[i]] = 17
        data[indicies_3[i]] = 17
        data[indicies_4[i]] = 17
        data[indicies_5[i]] = 17
        
        if indicies_1[i] > 0:
            data_1[indicies_1[i]] = data_1[indicies_1[i-1]]
        else:
            data_1[indicies_1[i]] = 17
            
        if indicies_2[i] > 0:
            data_1[indicies_2[i]] = data_1[indicies_2[i-1]]
        else:
            data_1[indicies_2[i]] = 17
            
        if indicies_3[i] > 0:
            data_1[indicies_3[i]] = data_1[indicies_3[i-1]]
        else:
            data_1[indicies_3[i]] = 17
            
        if indicies_4[i] > 0:
            data_1[indicies_4[i]] = data_1[indicies_4[i-1]]
        else:
            data_1[indicies_4[i]] = 17
            
        if indicies_5[i] > 0:
            data_1[indicies_5[i]] = data_1[indicies_5[i-1]]
        else:
            data_1[indicies_5[i]] = 17
    
    return data,data_1

def calculateEntropy(data):    
    deltas = [ checkQuery(data[0:i],data[i:len(data)]) for i in range(0,len(data)) ]    
    entropy = (numpy.log2(len(data)) * len(data))/sum(deltas)
    
    return entropy

def calculateUncorrelatedEntropy(data):
    
    dataLength=len(data)
    uniqueLocation = list(set(data))
    
    entropy = 0
    
    for i in range(0,len(uniqueLocation)):
        num = data.count(uniqueLocation[i])
        prob = float(num)/float(dataLength)
        
        if prob > 0:
            entropy = entropy + (prob)*numpy.log2(prob)
        
    return entropy*(-1)

def calculateStep(data,fraction):
    randData_1,randData_2 = randomizeData(data,fraction)
    
    entropy_1a = calculateEntropy(randData_1)
    entropy_1b = calculateUncorrelatedEntropy(randData_1)
    entropy_2a = calculateEntropy(randData_2)
    entropy_2b = calculateUncorrelatedEntropy(randData_2)
    
    returnData = []
    
    returnData.append(randData_1)
    returnData.append(randData_2)
    returnData.append(entropy_1a)
    returnData.append(entropy_1b)
    returnData.append(entropy_2a)
    returnData.append(entropy_2b)
    
    return returnData

def getOutput(result):
    output = "Data1 \n["
    
    data = result[0]
    
    output = "%s%i" % (output,int(data[0]))
    
    for index in range(1,len(data)):
        output = "%s,%i" % (output,int(data[index]))
    
    data = result[1]
    
    output = "%s]\nData2:\n[%i" % (output,int(data[0]))
    
    for index in range(1,len(data)):
        output = "%s,%i" % (output,int(data[index]))
        
    output = "%s]\n" % (output)
    
    output = "%sH(data1)=%f H_unc(data1)=%f H(data2)=%f H_unc(data2)=%f" % (output,float(result[2]),float(result[3]),float(result[4]),float(result[5]))
    
    return output

def checkData(data):
    
    uniqueLocation = list(set(data))
    
    output = "%i:%i" % (uniqueLocation[0],data.count(uniqueLocation[0]))
    
    for i in range(1,len(uniqueLocation)):
        num = data.count(uniqueLocation[i])
        output = "%s,%i:%i" % (output,uniqueLocation[i],num)
        
    return output

def getEntropyResult(result):
    output = "H(data1)=%f H_unc(data1)=%f H(data2)=%f H_unc(data2)=%f" % (float(result[2]),float(result[3]),float(result[4]),float(result[5]))
    
    return output
 
def Entropy_for_User(user):
    
    fields = ['user_id','pattern_length','entropy']
    
    dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
    
    result = dbHandler.select("SELECT max(event_id) FROM Events_BR_final WHERE zone_id = %i" % (user))
    
    max_event_id = 0
    
    for row in result:
        max_event_id = int(row[0])
    
    events = []
    raw_data = []
    
    for k in range(max_event_id):
        events.append([])
        raw_data.append(k+1)
    
    result = dbHandler.select("SELECT members,probability,pattern_id FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (user))
    
    singular_patterns = []
    
    for row in result:
        singular_patterns.append(float(row[1]))
        
        members = row[0][1:-1].split(",")
        
        for member in members:            
            events[int(member)-1].append(int(row[2])-1)    
    
    # Length 1 Patterns
    new_data = []
    
    for event in events:
        
        max_probability = 0
        best_pattern = -1
        
        for k in range(len(event)):
            if singular_patterns[event[k]] > max_probability:
                max_probability = singular_patterns[event[k]]
                best_pattern = event[k] + 1
                
        new_data.append(best_pattern)
    
    values = []
    values.append(user)
    values.append(0)
    values.append(calculateEntropy(raw_data))
    
    dbHandler.insert("Entropy",fields,values)
    
    values = []
    values.append(user)
    values.append(1)
    values.append(calculateEntropy(new_data))
    
    dbHandler.insert("Entropy",fields,values)
    """
    
    for pattern_length in range(2,3):
    
        pattern_base = []
        
        for k in range(len(singular_patterns)):
            pattern_base.append([])
            
        result = dbHandler.select("SELECT pattern_condition,pattern_members,probability*specificity,pattern_id FROM Pattern_Base WHERE pattern_length = %i AND " % (pattern_length))
                
        for row in result:            
            members = row[1][1:-1].split(",")
            members.append(float(row[2]))
            members.append(int(row[3]))
            pattern_base[int(row[0])-1].append(members)                
                        
        for k in range(len(pattern_base)):
            
            pattern_base[k].sort(key=itemgetter(2),reverse=True)
                
        
        data = []
        
        #result,entropy = Calculate_Entropy_Recursive(singular_patterns,pattern_base,events,data)
        Calculate_Entropy_Sequentially(singular_patterns,pattern_base,events[0:100])
                
        print entropy
        
        
        fields = ['user_id','pattern_length','entropy']
        
        values = []
        values.append(user)
        values.append(pattern_length)
        values.append(calculateEntropy(new_data))
        
        dbHandler.insert("Entropy",fields,values)
        
    """

def Calculate_Entropy_Recursive(singular_patterns,pattern_base,events,new_data):
    
    if len(events) == 0:
        entropy = calculateEntropy(new_data)
        return new_data,entropy
    else:    
        candidate_list = []
                        
        for k in range(len(events[0])):            
            if len(pattern_base[events[0][k]]) > 0:
                m = 0
                last_prob = 0
                
                for m in xrange(10):
                    candidate_list.append(pattern_base[events[0][k]][m])
                    
        candidate_list.sort(key=itemgetter(2),reverse=True)                
        length_events = len(events)
        
        if len(candidate_list) > 0:
            
            entropies = []
            results = []
            
            if len(new_data) == 0:
                pool = Pool(5)            
            pool_result = []
            
            for l in range(len(candidate_list)):
                if l < 5:
                    best_candidate = candidate_list[l]
                    
                    tmp_result = []
                    tmp_events = []
                    
                    for k in range(len(new_data)):
                        tmp_result.append(new_data[k])
                        
                    for k in range(len(events)):
                        tmp_events.append(events[k])
                    
                    tmp_events_length = len(tmp_events)
                    
                    for j in xrange(1,len(events)):
                        #print events[j]
                        if int(best_candidate[1])-1 in events[j]:
                            tmp_result.append(best_candidate[3])
                            tmp_events.pop(j)
                            tmp_events.pop(0)
                            break
                        
                    if len(tmp_events) < tmp_events_length:
                        if len(new_data) == 0:
                            pool_result.append(pool.apply_async(Calculate_Entropy_Recursive, [singular_patterns,pattern_base,tmp_events,tmp_result]))
                        else:
                            current_result,current_entropy = Calculate_Entropy_Recursive(singular_patterns,pattern_base,tmp_events,tmp_result)
                            results.append(current_result)
                            entropies.append(current_entropy)
                    else:
                        entropies.append(100000)
                        results.append([])                            
            
            for l in range(len(pool_result)):
                current_result,current_entropy = pool_result[l].get()
                results.append(current_result)
                entropies.append(current_entropy)
                
            lowest_entropy = 10000
            lowest_result = -1
                
            for l in range(len(candidate_list)):
                if l < 5:
                    if entropies[l] < lowest_entropy:
                        lowest_entropy = entropies[l]
                        lowest_result = l
            
            if lowest_result != -1:        
                best_candidate = candidate_list[lowest_result]
                
                for j in xrange(1,len(events)):                    
                    if int(best_candidate[1])-1 in events[j]:
                        new_data.append(best_candidate[3])
                        events.pop(j)
                        events.pop(0)
                        break
                
        if len(events) == length_events:
            
            max_probability = 0
            best_pattern = -1
            
            for k in range(len(events[0])):
                if singular_patterns[events[0][k]] > max_probability:
                    max_probability = singular_patterns[events[0][k]]
                    best_pattern = events[0][k] + 1
                    
            new_data.append(best_pattern)
            events.pop(0)
            
        return Calculate_Entropy_Recursive(singular_patterns,pattern_base,events,new_data)

def Calculate_Entropy_Sequentially(singular_patterns,pattern_base,event_list):
    
    data_list = []
    
    tmp = []
    tmp.append(event_list)
    tmp.append([])
    
    data_list.append(tmp)
    
    while any(len(data_list[k][0]) > 0 for k in range(len(data_list))):
        
        pool = Pool(len(data_list))            
        pool_result = []

        for i in range(len(data_list)):
            pool_result.append(pool.apply_async(Entropy_Sequentially_Thread, [singular_patterns,pattern_base,data_list,i]))
            
        
        data_list = []
            
        for l in range(len(pool_result)):
            result_data = pool_result[l].get()
            
            for k in range(len(result_data)):
                data_list.append(result_data[k])
                
        print len(data_list)
                
    print len(data_list)
                
def Entropy_Sequentially_Thread(singular_patterns,pattern_base,data_list,i):
    
    result_data = []
    
    events = []
    result = []
    
    for k in range(len(data_list[i][0])):
        events.append(data_list[i][0][k])
        
    for k in range(len(data_list[i][1])):
        result.append(data_list[i][1][k])

    candidate_list = []
                    
    for k in range(len(events[0])):            
        if len(pattern_base[events[0][k]]) > 0:
            m = 0
            last_prob = 0
            
            for m in xrange(10):
                candidate_list.append(pattern_base[events[0][k]][m])
                
    candidate_list.sort(key=itemgetter(2),reverse=True)                    
    
    if len(candidate_list) > 0:
        
        entropies = []
        results = []
        
        for l in range(len(candidate_list)):
            if l < 2:
                best_candidate = candidate_list[l]
                
                tmp_result = []
                tmp_events = []
                
                for k in range(len(result)):
                    tmp_result.append(result[k])
                    
                for k in range(len(events)):
                    tmp_events.append(events[k])
                
                tmp_events_length = len(tmp_events)
                
                for j in xrange(1,len(events)):
                    #print events[j]
                    if int(best_candidate[1])-1 in events[j]:
                        tmp_result.append(best_candidate[3])
                        tmp_events.pop(j)
                        tmp_events.pop(0)
                        break
                    
                if len(tmp_events) < tmp_events_length:
                    tmp = []
                    tmp.append(tmp_events)
                    tmp.append(tmp_result)
                    result_data.append(tmp)                
                    
    
    if len(result_data) == 0:
        max_probability = 0
        best_pattern = -1
        
        for k in range(len(events[0])):
            if singular_patterns[events[0][k]] > max_probability:
                max_probability = singular_patterns[events[0][k]]
                best_pattern = events[0][k] + 1
                
        result.append(best_pattern)
        events.pop(0)
        
        tmp = []
        tmp.append(events)
        tmp.append(result)
        result_data.append(tmp)
        
    return result_data

def Entropy_Analysis():
    
    dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Building_Robotics_Pattern_Base")
    
    user_ids = []
    
    result = dbHandler.select("SELECT DISTINCT user_id FROM Entropy")
    
    for row in result:
        user_ids.append(int(row[0]))
        
    fields = ['user_id','entropy_change']
    
    for user in user_ids:
        
        entropy = []
        entropy.append(0)
        entropy.append(0)
        
        result = dbHandler.select("SELECT * FROM Entropy WHERE user_id = %i" % (user))
        
        for row in result:
            entropy[int(row[2])] = float(row[3])
                    
        dbHandler.insert("Entropy_Analysis",fields,[user,(1-float(entropy[1]/entropy[0]))])
        
# Main Program
if __name__ == "__main__":
    
    sys.setrecursionlimit(15000)
    #Entropy_for_User(177)
        
    Entropy_Analysis()
    
    """
    dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Building_Robotics_Pattern_Base")
    
    user_ids = []
    
    result = dbHandler.select("SELECT DISTINCT user_id FROM Singular_Pattern_Base")
    
    for row in result:
        user_ids.append(int(row[0]))
    
    for user in user_ids:
        Entropy_for_User(user)
        
    """