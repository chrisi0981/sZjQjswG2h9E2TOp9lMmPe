#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys
from multiprocessing import Process,Manager
from multiprocessing import Pool
import os
import thread
import threading

import random
import itertools

import time
import datetime
from pytz import timezone

import numpy
from collections import Counter
from operator import itemgetter
import pickle
import gc

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Extract_Zone_Events_BR(zone):
		
	fields = ['zone_id','originating_user','activity','timestamp','date','time','time_index','week_weekend','dow','dom','doy','number_in_month','current_temperature','blast_action','event_id']
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")		
	
	data_result = dbHandler.select("SELECT location_id,user_id,type,timestamp,date,time,FLOOR((HOUR(time)*60 + MINUTE(time))/5),WEEKDAY(date),DAYOFMONTH(date),DAYOFYEAR(date),CEIL(DAYOFMONTH(date)/7),temp,blast_action FROM complaints WHERE location_id = %i ORDER BY timestamp" % (zone))
	
	last_date = ""	
	event_id = 1
	last_values = []
	
	for row in data_result:
		
		if last_date == "":
			values = []
			values.append(zone)
			values.append(row[1])
			values.append(Get_Activity(int(row[2])))
			values.append(row[3])
			values.append(row[4])
			values.append(row[5])
			values.append(row[6])
			
			if int(row[7]) < 5:
				values.append(1)
			else:
				values.append(0)
				
			values.append(row[7])
			values.append(row[8])
			values.append(row[9])
			values.append(row[10])
			values.append(row[11])
			
			if row[12] != '':
				values.append(1)
			else:
				values.append(0)
				
			values.append(event_id)
			event_id = event_id + 1
				
			dbHandler.insert("Events_BR",fields,values)
			last_values = values
			
			values = []
			values.append(zone)
			values.append(row[1])
			values.append("First Interaction")
			values.append(row[3])
			values.append(row[4])
			values.append(row[5])
			values.append(row[6])
			
			if int(row[7]) < 5:
				values.append(1)
			else:
				values.append(0)
				
			values.append(row[7])
			values.append(row[8])
			values.append(row[9])
			values.append(row[10])
			values.append(row[11])
			
			if row[12] != '':
				values.append(1)
			else:
				values.append(0)
				
			values.append(event_id)
			event_id = event_id + 1
				
			dbHandler.insert("Events_BR",fields,values)
		else:
			if last_date != row[4]:
				last_values[2] = "Last Interaction"
				last_values[14] = event_id
				event_id = event_id + 1
				dbHandler.insert("Events_BR",fields,last_values)
			
				values = []
				values.append(zone)
				values.append(row[1])
				values.append(Get_Activity(int(row[2])))
				values.append(row[3])
				values.append(row[4])
				values.append(row[5])
				values.append(row[6])
				
				if int(row[7]) < 5:
					values.append(1)
				else:
					values.append(0)
					
				values.append(row[7])
				values.append(row[8])
				values.append(row[9])
				values.append(row[10])
				values.append(row[11])
				
				if row[12] != '':
					values.append(1)
				else:
					values.append(0)
					
				values.append(event_id)
				event_id = event_id + 1
					
				dbHandler.insert("Events_BR",fields,values)
				last_values = values
				
				last_values[2] = "First Interaction"
				last_values[14] = event_id
				event_id = event_id + 1
				dbHandler.insert("Events_BR",fields,last_values)
				
			else:
				values = []
				values.append(zone)
				values.append(row[1])
				values.append(Get_Activity(int(row[2])))
				values.append(row[3])
				values.append(row[4])
				values.append(row[5])
				values.append(row[6])
				
				if int(row[7]) < 5:
					values.append(1)
				else:
					values.append(0)
					
				values.append(row[7])
				values.append(row[8])
				values.append(row[9])
				values.append(row[10])
				values.append(row[11])
				
				if row[12] != '':
					values.append(1)
				else:
					values.append(0)
					
				values.append(event_id)
				event_id = event_id + 1
					
				dbHandler.insert("Events_BR",fields,values)
				last_values = values
			
		last_date = row[4]
				
					
			
def Get_Activity(request_type):
	
	if request_type == 1:
		return "Too Hot"
	
	if request_type == 2:
		return "Too Cold"
	
	if request_type == 3:
		return "Comfortable"
	
	if request_type == 4:
		return "Cancel"

def Extract_Temporal_Cluster_Zone_BR(zone): # The number of minutes a new event can differ from mean of previous events (5-minute increments)
		
	event_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	activities = ['Too Hot','Too Cold','Comfortable','Cancel','First Interaction','Last Interaction']
	#activities = ['Too Hot']
	
	
	for activity in activities:
		
		try:
			avg_cluster_size = []
			avg_between_cluster_distance = []
			avg_cluster_density = []
			
			std_cluster_size = []
			std_between_cluster_distance = []
			std_cluster_density = []
			
			num_clusters = []
			num_short_clusters = []
			
			dunn_indices = []
			davies_bouldin_indices = []
			
			cluster_result = []
			cluster_result_means = []
			
			for outer_threshold in range(1,8):
				for inner_threshold in range(1,8):
					
					#print activity,location,outer_threshold,inner_threshold
					
					result = event_dbHandler.select("SELECT * FROM Events_BR WHERE activity = '%s' AND zone_id = %i ORDER BY time_index" % (activity,zone))
					
					clusters = []
					cluster_means = []
					
					event_ids = []
					event_data = []
					
					num_weekdays = 0
					num_weekends = 0
					weekdays = []
					
					for row in result:
						
						event_ids.append(int(row[16]))
						event_data.append(list(row))
						
						weekdays.append(int(row[9]))
						
						if int(row[8]) == 1:
							num_weekdays = num_weekdays + 1
						else:
							num_weekends = num_weekends + 1
						
						if len(clusters) == 0:
							new_cluster = []
							new_cluster.append(int(row[16]))
							clusters.append(new_cluster)
							
							new_mean = []
							new_mean.append(int(row[7]))
							cluster_means.append(new_mean)				
						else:
							tmp_means = []
							
							for mean in cluster_means:
								tmp_means.append(numpy.mean(numpy.array(mean)))
											
							closest_index = Find_Nearest(numpy.array(tmp_means),int(row[7]))
							
							if int(row[1]) < 120 or int(row[1]) > 192:
								threshold = outer_threshold
							else:
								threshold = inner_threshold
							
							if math.fabs(int(row[7])-tmp_means[closest_index]) <= threshold:
								new_cluster = clusters[closest_index]
								new_cluster.append(int(row[16]))
								clusters[closest_index] = new_cluster
								
								new_mean = cluster_means[closest_index]
								new_mean.append(int(row[7]))
								cluster_means[closest_index] = new_mean
							else:
								new_cluster = []
								new_cluster.append(int(row[16]))
								clusters.append(new_cluster)
								
								new_mean = []
								new_mean.append(int(row[7]))
								cluster_means.append(new_mean)
							
					
					cluster_length = []
					short_clusters = 0
					cluster_densities = []
					
					for i in range(len(clusters)):
						cluster_length.append(len(clusters[i]))
						
						if len(clusters[i]) < 5:
							short_clusters = short_clusters + 1
						
						data = []
						days = []
						
						cluster_densities.append(Calculate_Cluster_Density(cluster_means[i]))
						
						for event in clusters[i]:
							closest_index = Find_Nearest(numpy.array(event_ids),event)
							
							tmp_data = event_data[closest_index]
							data_string = "%i,%i,%i" % (int(tmp_data[7]),int(tmp_data[10]),int(tmp_data[8]))
							data.append(data_string)
							days.append(tmp_data[10])					
					
					previous_mean = 0
					between_cluster_distances = []
					
					for values in cluster_means:
						
						current_mean = numpy.mean(numpy.array(values))
						
						if previous_mean == 0:
							previous_mean = current_mean
						else:
							between_cluster_distances.append(current_mean-previous_mean)
							previous_mean = current_mean
					
					#print between_cluster_distances
					avg_between_cluster_distance.append(numpy.mean(numpy.array(between_cluster_distances)))
					std_between_cluster_distance.append(numpy.std(numpy.array(between_cluster_distances)))
					
					num_clusters.append(len(clusters))
					num_short_clusters.append(short_clusters)
					#print len(event_ids),numpy.mean(numpy.array(cluster_length)),numpy.std(numpy.array(cluster_length))
					avg_cluster_size.append(numpy.mean(numpy.array(cluster_length)))
					std_cluster_size.append(numpy.std(numpy.array(cluster_length)))
					
					avg_cluster_density.append(numpy.mean(numpy.array(cluster_densities)))
					std_cluster_density.append(numpy.std(numpy.array(cluster_densities)))
					
					max_density = cluster_densities[numpy.argmax(numpy.array(cluster_densities))]
					
					dunn_candidates = []				
					
					for i in range(len(cluster_means)):
						for j in range(i+1,len(cluster_means)):
							if max_density > 0:
								dunn_candidates.append(float(math.fabs(numpy.mean(numpy.array(cluster_means[i]))-numpy.mean(numpy.array(cluster_means[j]))))/float(max_density))
							else:
								dunn_candidates.append(0)
	
					if len(dunn_candidates) > 0:
						dunn_indices.append(dunn_candidates[numpy.argmin(numpy.array(dunn_candidates))])
					else:
						dunn_indices.append(0)
					
					db = 0
					
					for i in range(len(cluster_means)):
						db_tmp = []
						
						for j in range(len(cluster_means)):
							if i != j:
								if float(math.fabs(numpy.mean(numpy.array(cluster_means[i]))-numpy.mean(numpy.array(cluster_means[j])))) > 0:
									db_tmp.append(float(cluster_densities[i]+cluster_densities[j])/float(math.fabs(numpy.mean(numpy.array(cluster_means[i]))-numpy.mean(numpy.array(cluster_means[j])))))
								else:
									db_tmp.append(0)
							
						db = db + db_tmp[numpy.argmax(numpy.array(db_tmp))]
						
					if float(len(clusters)) > 0:
						davies_bouldin_indices.append(float(db)/float(len(clusters)))
					else:
						davies_bouldin_indices.append(0)
					
					cluster_result.append(clusters)
					
					mean_tmp = []
					
					for i in range(len(cluster_means)):
						mean_tmp.append(numpy.mean(numpy.array(cluster_means[i])))
					
					cluster_result_means.append(mean_tmp)
			
			output = ""
			
			fields = ['user_id','activity','outer_temporal_threshold','inner_temporal_threshold','mean_cluster_distance','mean_cluster_diameter','mean_cluster_size','std_cluster_distance','std_cluster_diameter','std_cluster_size','number_clusters','number_short_clusters','dunn_index','davies_bouldin_index']
			
			event_dbHandler.deleteData("DELETE FROM Pattern_Analysis WHERE user_id = %i" % (zone))
			
			for outer_threshold in range(1,8):
				for inner_threshold in range(1,8):				
					values = []
					values.append(zone)
					values.append(activity)		
					values.append(outer_threshold*5)
					values.append(inner_threshold*5)
					values.append(avg_between_cluster_distance[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(avg_cluster_density[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(avg_cluster_size[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(std_between_cluster_distance[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(std_cluster_density[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(std_cluster_size[(outer_threshold-1)*7+(inner_threshold-1)])		
					values.append(num_clusters[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(num_short_clusters[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(dunn_indices[(outer_threshold-1)*7+(inner_threshold-1)])
					values.append(davies_bouldin_indices[(outer_threshold-1)*7+(inner_threshold-1)])
					
					event_dbHandler.insert("Pattern_Analysis",fields,values)
					
			
			outer_threshold,inner_threshold = Find_Best_Clustering(zone,activity)
			
			best_cluster = cluster_result[(outer_threshold-1)*7+(inner_threshold-1)]
			best_cluster_means = cluster_result_means[(outer_threshold-1)*7+(inner_threshold-1)]										
			
			fields = ['zone_id','originating_user','activity','timestamp','date','time','time_index','week_weekend','season','dow','dom','doy','number_in_month','current_temperature','blast_action','event_id','singular_pattern_id','temporal_cluster_id','temporal_cluster_centroid','temporal_cluster_threshold']			
			
			event_result = event_dbHandler.select("SELECT * FROM Events_BR WHERE zone_id = %i AND activity = '%s'" % (zone,activity))
			
			for event in event_result:			
				for i in range(len(best_cluster)):				
					try:
						best_cluster[i].index(int(event[16]))
																
						current_event = list(event)						
						current_event[18] = Get_Cluster_ID(activity,zone,i+1)
						current_event[19] = best_cluster_means[i]
						current_event[20] = "%i:%i" % (outer_threshold*5,inner_threshold*5)
						current_event.pop(0)						
						event_dbHandler.insert("Events_BR_final",fields,current_event)
					except ValueError, e:
						pass
		except:
			pass
	return True
				
def Extract_Singular_Patterns_BR(zone):
	
	event_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")

	dates = []
	num_weekdays = []
	num_dom = []
	num_number_in_month = []
	num_week_weekends = []
	num_season = []
	num_blast_action = []
	
	event_dbHandler.truncateTable("Events_BR_final_tmp")
	event_dbHandler.update("INSERT INTO Events_BR_final_tmp SELECT * FROM Events_BR_final WHERE zone_id = %i" % (zone))
	
	result = event_dbHandler.select("SELECT DISTINCT date,dow,dom,number_in_month,week_weekend,season,blast_action FROM Events_BR_final_tmp")

	for row in result:
		dates.append(row[0])
		num_weekdays.append(int(row[1]))
		num_dom.append(int(row[2]))
		num_number_in_month.append(int(row[3]))
		num_week_weekends.append(int(row[4]))
		num_season.append(int(row[5]))
		num_blast_action.append(int(row[6]))
	
	weekdays_hist,tmp = numpy.histogram(num_weekdays,bins=[0,1,2,3,4,5,6,7])
	weekends_hist,tmp = numpy.histogram(num_week_weekends,bins=[0,1,2])
	month_hist,tmp = numpy.histogram(num_dom,bins=range(1,33))
	season_hist,tmp = numpy.histogram(num_season,bins=[0,1,2,3,4])
	blast_action_hist,tmp = numpy.histogram(num_blast_action,bins=[0,1,2])
	
	nim = []	
	
	for i in range(7):
		tmp = numpy.zeros(5,int)
		nim.append(tmp)
	
	for i in range(len(num_number_in_month)):				
		tmp = nim[num_weekdays[i]]		
		tmp[num_number_in_month[i]-1] = tmp[num_number_in_month[i]-1] + 1
		
		nim[num_weekdays[i]] = tmp
	
	result = event_dbHandler.select("SELECT DISTINCT temporal_cluster_id, temporal_cluster_centroid FROM Events_BR_final_tmp")
	
	event_clusters = []
	event_cluster_centroids = []
	
	for row in result:
		event_clusters.append(row[0])
		event_cluster_centroids.append(int(row[1]))
		
	pattern_id = 1
	
	fields = ['user_id','pattern_id','activity','location','time','members','probability','dow','dom','doy','season','week_weekend','number_in_month','blast_action']
	
	for i in range(len(event_clusters)):
		
		event_ids = []
		activity = ""
		location = -1
		dow = []
		dom = []
		doy = []
		season = []
		week_weekend = []
		number_in_month = []
		blast_action = []
		
		count = 0
		last_doy = 0
		
		result = event_dbHandler.select("SELECT * FROM Events_BR_final_tmp WHERE temporal_cluster_id = '%s' ORDER BY doy" % (event_clusters[i]))
		
		for row in result:			
			event_ids.append(int(row[16]))
			activity = row[3]
			location = zone
			dow.append(int(row[10]))
			dom.append(int(row[11]))
			doy.append(int(row[12]))
			season.append(int(row[9]))						
			week_weekend.append(int(row[8]))			
			number_in_month.append(int(row[13]))
			blast_action.append(int(row[15]))
			
			if last_doy != int(row[12]):
				count = count + 1
			
			last_doy = int(row[12])
					
		
		# Singular Pattern (SP) without contextual information		
		values = []
		values.append(zone)
		values.append(pattern_id)
		values.append(activity)
		values.append(location)
		values.append(event_cluster_centroids[i])
		values.append(Get_Member_List(event_ids,range(len(event_ids))))
		
		if len(dates) > 0:
			values.append(float(count)/float(len(dates)))
		else:
			values.append(0)
		
		pattern_id = pattern_id + 1
		
		event_dbHandler.insert("Singular_Pattern_Base",fields[0:7],values)
		
		# SP for Day of Week
		hist,tmp = numpy.histogram(dow,bins=[0,1,2,3,4,5,6,7])
		
		for weekday in range(7):
			if hist[weekday] != 0:
				reps = Find_Elements(dow,weekday)
				
				values = []
				values.append(zone)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(weekdays_hist[weekday]) > 0:
					values.append(float(Probability_Count(doy,dow,weekday))/float(weekdays_hist[weekday])) # Probability
				else:					
					values.append(0)
					
				values.append(weekday)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[7])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		
		# SP for Day of Month
		hist,tmp = numpy.histogram(dom,bins=range(1,33))
		
		for dayOfMonth in range(1,32):									
			reps = Find_Elements(dom,dayOfMonth)
			
			if len(reps) > 0:
			
				values = []
				values.append(zone)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(month_hist[dayOfMonth-1]) > 0:
					values.append(float(Probability_Count(doy,dom,dayOfMonth))/float(month_hist[dayOfMonth-1])) # Probability
				else:
					values.append(0)
					
				values.append(dayOfMonth)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[8])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Blast Action
		hist,tmp = numpy.histogram(blast_action,bins=[0,1,2])
		
		for current_blast_action in range(2):
			if hist[current_blast_action] != 0:
				reps = Find_Elements(blast_action,current_blast_action)
			
				values = []
				values.append(zone)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(blast_action_hist[current_blast_action]) > 0:
					values.append(float(Probability_Count(doy,blast_action,current_blast_action))/float(blast_action_hist[current_blast_action])) # Probability
				else:
					values.append(0)
					
				values.append(current_blast_action)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[13])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
		
		# SP for Season
		hist,tmp = numpy.histogram(season,bins=[0,1,2,3,4])
		
		for current_season in range(4):
			if hist[current_season] != 0:
				reps = Find_Elements(season,current_season)
			
				values = []
				values.append(zone)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(season_hist[current_season]) > 0:
					values.append(float(Probability_Count(doy,season,current_season))/float(season_hist[current_season])) # Probability
				else:
					values.append(0)
					
				values.append(current_season)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[10])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Week vs. Weekend
		hist,tmp = numpy.histogram(week_weekend,bins=[0,1,2])
		
		for current_week_weekend in range(2):
			if hist[current_week_weekend] != 0:
				reps = Find_Elements(week_weekend,current_week_weekend)
			
				values = []
				values.append(zone)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(weekends_hist[current_week_weekend]) > 0:
					values.append(float(Probability_Count(doy,week_weekend,current_week_weekend))/float(weekends_hist[current_week_weekend])) # Probability
				else:
					values.append(0)
					
				values.append(current_week_weekend)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[11])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Number in Month
		cluster_nim = []
	
		for l in range(7):			
			tmp = numpy.zeros(5,int)
			cluster_nim.append(tmp)
		
		last_doy = -1
		
		for l in range(len(number_in_month)):							
			if last_doy != doy[l]:			
				tmp = cluster_nim[dow[l]]		
				tmp[number_in_month[l]-1] = tmp[number_in_month[l]-1] + 1
				
				cluster_nim[dow[l]] = tmp
				
			last_doy = doy[l]
		
		for weekday in range(7):
			for number in range(1,6):
				if cluster_nim[weekday][number-1] != 0:
					
					reps = Find_Elements(dow,weekday)
					
					member_list = ","
					
					for l in range(len(reps)-1):
						if number_in_month[reps[l]] == number:
							member_list = "%s%i," % (member_list,event_ids[reps[l]])
		
					if number_in_month[reps[-1]] == number:
						member_list = "%s%i," % (member_list,event_ids[reps[-1]])
			
					values = []
					values.append(zone)
					values.append(pattern_id)
					values.append(activity)
					values.append(location)
					values.append(event_cluster_centroids[i])
					values.append(member_list)
					
					if float(nim[weekday][number-1]) > 0:
						values.append(float(cluster_nim[weekday][number-1])/float(nim[weekday][number-1])) # Probability
					else:
						values.append(0)
						
					values.append(weekday)
					values.append(number)
					
					pattern_id = pattern_id + 1
					
					new_fields = fields[0:7]
					new_fields.append(fields[7])				
					new_fields.append(fields[12])				
					
					event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
		

def Map_Singular_Patterns_BR():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	data_fields = ['id','zone_id','originating_user','activity','timestamp','date','time','time_index','week_weekend','season','dow','dom','doy','number_in_month','current_temperature','blast_action','event_id','singular_pattern_id','temporal_cluster_id','temporal_cluster_centroid','temporal_cluster_threshold']
	
	user_ids = []
	
	result = dbHandler.select("SELECT DISTINCT user_id FROM Singular_Pattern_Base")
	
	for row in result:
		user_ids.append(int(row[0]))
	
	for user in user_ids:
		start = time.time()
		
		dbHandler.truncateTable("Singular_Pattern_Base_tmp")		
		dbHandler.select("INSERT INTO Singular_Pattern_Base_tmp SELECT * FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
		
		result = dbHandler.select("SELECT * FROM Events_BR_final WHERE zone_id = %i ORDER BY timestamp" % (user))
		
		for row in result:
			
			sp_ids = []
			
			select_string = "SELECT pattern_id FROM Singular_Pattern_Base_tmp WHERE user_id =  %i AND INSTR(members,',%i,') > 0 " % (user,int(row[16]))
			
			sp_result = dbHandler.select(select_string)
			
			for sp_row in sp_result:
				sp_ids.append(int(sp_row[0]))
					
			sp_output = ","
			
			for i in range(len(sp_ids)):
				sp_output = "%s%i," % (sp_output,sp_ids[i])
			
			values = list(row)
			values[17] = sp_output
			
			dbHandler.insert("Events_BR_mapped",data_fields,values)
			
		print time.time()-start

def Extract_Conditional_Patterns(zone,max_interval_length,pool):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")	
	
	fields = ["user_id","pattern_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	dbHandler.truncateTable("Events_BR_mapped_tmp")
	dbHandler.update("INSERt INTO Events_BR_mapped_tmp SELECT * FROM Events_BR_mapped WHERE zone_id = %i" % (zone))	
	dbHandler.createTable("Pattern_Base_%i" % (zone),"SELECT * FROM Pattern_Base_template")
	dbHandler.update("ALTER TABLE Pattern_Base_%i ENGINE = MyISAM" % (zone))
	dbHandler.createTable("Valid_Instances_%i" % (zone),"SELECT * FROM Valid_Instances")
	dbHandler.update("ALTER TABLE Valid_Instances_%i ENGINE = MyISAM" % (zone))
	
	temporal_interval_description = []
	temporal_interval_specificity = []
		
	temporal_interval_description.append("same_month")
	temporal_interval_description.append("within_adjacent_2_week")
	temporal_interval_description.append("within_adjacent_1_week")
	temporal_interval_description.append("same_week")
	temporal_interval_description.append("within_adjacent_2_day")
	temporal_interval_description.append("within_adjacent_1_day")
	temporal_interval_description.append("same_day")
	
	for k in range(1,8):
		temporal_interval_specificity.append(0.14285714*k)
				
	singular_pattern_id = []
	singular_pattern_specificity = []
	
	result = dbHandler.select("SELECT DISTINCT pattern_id,specificity FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (zone))
	
	for row in result:
		singular_pattern_id.append(int(row[0]))
		singular_pattern_specificity.append(int(row[1]))
	
	timestamps = []
	dates = []
	weeks = []
	months = []
	
	for k in range(max(singular_pattern_id)):
		timestamps.append(-1)
		dates.append("")
		weeks.append(-1)
		months.append(-1)
	
	for sp_pattern_id in singular_pattern_id:
			
		result = dbHandler.select("SELECT timestamp,date,WEEK(date),MONTH(date) FROM Events_BR_mapped_tmp WHERE INSTR(singular_pattern_id,',%i,') > 0 GROUP BY date ORDER BY timestamp" % (sp_pattern_id))
		
		timestamps_tmp = []
		dates_tmp = []
		weeks_tmp = []
		months_tmp = []
		
		for row in result:
			timestamps_tmp.append(int(row[0]))
			dates_tmp.append(row[1])
			weeks_tmp.append(int(row[2]))
			months_tmp.append(int(row[3]))
			
		timestamps[sp_pattern_id-1] = (timestamps_tmp)
		dates[sp_pattern_id-1] = (dates_tmp)
		weeks[sp_pattern_id-1] = (weeks_tmp)
		months[sp_pattern_id-1] = (months_tmp)
	
	singular_patterns = []
	
	result = dbHandler.select("SELECT probability FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 3 AND user_id = %i" % (zone))
	
	probabilities_sp = []
	
	for row in result:
		probabilities_sp.append(float(row[0]))
		
	prob_median = numpy.median(probabilities_sp)
	
	result = dbHandler.select("SELECT * FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 3 AND user_id = %i ORDER BY pattern_id" % (zone))
	
	current_sp_collection = []
	singular_patterns = []
	singular_patterns_collection = []	
	non_duration_group = []
	non_duration_group_int = []
	
	last_activity = ""
	last_location = -1
	last_time = -1
	
	for row in result:
		
		singular_patterns.append(row[2])
		
		non_duration_group.append(row[2])
		non_duration_group_int.append(int(row[2]))
		
		if last_activity == "":
			last_activity = row[3]
			last_location = row[4]
			last_time = row[5]
			
		if last_activity == row[3] and last_location == row[4] and last_time == row[5]:
			current_sp_collection.append(row[2])
		else:
			singular_patterns_collection.append(current_sp_collection)
			current_sp_collection = []
			current_sp_collection.append(row[2])
			
		last_activity = row[3]
		last_location = row[4]
		last_time = row[5]
	
	singular_patterns_collection.append(current_sp_collection)	
	
	current_pattern_id = len(singular_pattern_id) + 1
	
	pattern_combinations = []
	pattern_combinations_temporal_intervals = []
	pattern_combinations_temporal_specificity = []
	pattern_combinations_count = []
		
	for pattern_length in range(2,10):						
		
		del pattern_combinations
		del pattern_combinations_temporal_intervals
		del pattern_combinations_temporal_specificity
		del pattern_combinations_count
		
		gc.collect()
		
		pattern_combinations = []
		pattern_combinations_temporal_intervals = []
		pattern_combinations_temporal_specificity = []
		pattern_combinations_count = []
		
		target_index = 0
		count = 0
		
		if pattern_length == 2:
			singular_pattern_count = []
			
			for k in range(len(singular_pattern_id)):
				result = dbHandler.select("SELECT count(id) FROM Events_BR_mapped_tmp WHERE zone_id = %i AND INSTR(singular_pattern_id,',%s,') > 0" % (zone,singular_pattern_id[k]))
				
				for row in result:
					singular_pattern_count.append(float(row[0]))
			
			combinations = itertools.permutations(range(len(singular_patterns)),2)						
			
			for comb in combinations:
				comb = list(comb)
				
				valid = True
				
				if singular_patterns[comb[-1]] in non_duration_group:
					for sp_collection in singular_patterns_collection:
						if all(singular_patterns[comb[k]] in sp_collection for k in range(len(comb))):
							valid = False
				
				if valid:
					tmp = []
					most_specific = singular_pattern_specificity[singular_patterns[comb[-1]]-1]
					
					for k in range(len(comb)):
						tmp.append(singular_patterns[comb[k]])
										
					pattern_combinations.append(tmp)
					
					new_temporal_interval_description = []
					new_temporal_interval_specificity = []
					
					if most_specific == 0:
						
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
							
					if most_specific == 1:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
					
					if most_specific == 2:
						for k in range(3,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 3:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 4:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 5:
						for k in range(0,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 6:
						for k in range(0,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
					
					pattern_combinations_temporal_intervals.append(new_temporal_interval_description)
					pattern_combinations_temporal_specificity.append(new_temporal_interval_specificity)					
					
					sp_count = []
					for k in range(len(new_temporal_interval_description)):
						sp_count.append(singular_pattern_count[tmp[0]-1])
						
					pattern_combinations_count.append(sp_count)
					
		else:
			result = dbHandler.select("SELECT DISTINCT pattern_members FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = %i"  % (zone,pattern_length-1))
			
			valid_patterns = {}
			valid_patterns_2 = {}
			
			for row in result:
				members = row[0][1:-1].split(",")
				
				try:
					current_values = valid_patterns[','.join(members[0:len(members)-1])]
					current_values.append(members[-1])
					valid_patterns[','.join(members[0:len(members)-1])] = current_values
				except KeyError, e:
					tmp = []
					tmp.append(members[-1])
					valid_patterns[','.join(members[0:len(members)-1])] = tmp
			
			result = dbHandler.select("SELECT DISTINCT pattern_members FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = 2"  % (zone))
						
			valid_patterns_2 = {}
			
			for row in result:
				members = row[0][1:-1].split(",")
				
				try:
					current_values = valid_patterns_2[','.join(members[0:1])]
					current_values.append(members[-1])
					valid_patterns_2[','.join(members[0:1])] = current_values
				except KeyError, e:
					tmp = []
					tmp.append(members[-1])
					valid_patterns_2[','.join(members[0:1])] = tmp
						
			result = dbHandler.select("SELECT * FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = %i" % (zone,pattern_length-1))
			
			last_pattern = ""
			
			current_pattern_temp_intervals = []
			current_pattern_temp_specificity = []
			current_sp_count = []
			
			test_patterns = []
			
			in_duration_group = 0
			
			for row in result:
				
				if last_pattern == "":
					last_pattern = row[3]
					current_pattern_temp_intervals.append(row[5])
					current_pattern_temp_specificity.append(row[9])
					current_sp_count.append(int(row[8]))
				else:
					if last_pattern == row[3]:
						current_pattern_temp_intervals.append(row[5])
						current_pattern_temp_specificity.append(row[9])
						current_sp_count.append(int(row[8]))
					else:
						try:
							members = last_pattern[1:-1].split(",")
							value_index = ','.join(members[1:len(members)])
							
							current_values = valid_patterns[value_index]
							
							for value in current_values:								
								members = last_pattern[1:-1].split(",")
								members.append(value)
								pattern_combinations.append(members)
								pattern_combinations_temporal_intervals.append(current_pattern_temp_intervals)
								pattern_combinations_temporal_specificity.append(current_pattern_temp_specificity)
								pattern_combinations_count.append(current_sp_count)
							
						except KeyError, e:
							error = ""
						
						current_pattern_temp_intervals = []
						current_pattern_temp_specificity = []
						current_sp_count = []
						
						last_pattern = row[3]
						current_pattern_temp_intervals.append(row[5])
						current_pattern_temp_specificity.append(row[9])
						current_sp_count.append(int(row[8]))
											
		threads = []				
		
		max_pattern = len(pattern_combinations)
		#max_pattern = 100		
		
		start = time.time()
		
		max_prob_calc = 0
		
		for temp_intervals in pattern_combinations_temporal_intervals:
			max_prob_calc = max_prob_calc + len(temp_intervals)
			
		print max_prob_calc
		
		num_threads = 3
		
		if max_prob_calc < 20000000 and max_prob_calc > 0:
		
			print "Start calculating probabilities!!!"						
			
			for k in range(num_threads):			
				if k != num_threads-1:
					lower = int((k)*math.floor(float(max_pattern)/float(num_threads)))
					upper = int((k+1)*math.floor(float(max_pattern)/float(num_threads)))
				else:
					lower = int((k)*math.floor(float(max_pattern)/float(num_threads)))
					upper = max_pattern
			
				if pattern_length > 2:
					probability_thread = pool.apply_async(Calculate_Probability, (pattern_combinations[lower:upper],pattern_combinations_temporal_intervals[lower:upper],pattern_combinations_temporal_specificity[lower:upper],pattern_combinations_count[lower:upper],pattern_length,zone,0,len(pattern_combinations[lower:upper]),months,weeks,dates,timestamps,temporal_interval_description,) )
				else:					
					probability_thread = pool.apply_async(Calculate_Probability_Length2, (pattern_combinations[lower:upper],pattern_combinations_temporal_intervals[lower:upper],pattern_combinations_temporal_specificity[lower:upper],pattern_combinations_count[lower:upper],pattern_length,zone,0,len(pattern_combinations[lower:upper]),months,weeks,dates,timestamps,temporal_interval_description,singular_pattern_id,) )
				threads.append(probability_thread)				
						
			for current_thread in threads:
				current_thread.get()
			
			print time.time() - start
		else:
			error_log = open("error_log.txt","a")
			error_log.write("Stopped for %i at pattern length %i; Probability count: %i\n" % (zone,pattern_length,max_prob_calc))
			error_log.close()
			
			break
		
					
	
def Calculate_Probability(pattern_combinations,pattern_combinations_temporal_intervals,pattern_combinations_temporal_specificity,pattern_combinations_count,pattern_length,user,lower,upper,months,weeks,dates,timestamps,temporal_interval_description):
	
	print "		Calculate Probabilities for user %i from pattern %s to pattern %s (%i,%i)" % (user,pattern_combinations[lower],pattern_combinations[upper-1],lower,upper)
	
	#dbHandler_pattern = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	fields = ["user_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	valid_instances = []
	gc.collect()

	valid_instances = Get_Valid_Instances(pattern_length-1,user)
	#dbHandler.truncateTable("Valid_Instances")
	
	instances_bulk = []
	bulk_insert = []
	
	for l in range(lower,upper):
		
		if len(instances_bulk) >= 5000:
			dbHandler.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			del instances_bulk
			instances_bulk = []
				
		pattern_comb = pattern_combinations[l]
		pattern_intervals = pattern_combinations_temporal_intervals[l]
		pattern_specificities = pattern_combinations_temporal_specificity[l]
		
		counts = numpy.zeros(7,int)
		
		start = time.time()
		
		for m in range(len(pattern_intervals)):
			count_index = temporal_interval_description.index(pattern_intervals[m])
			conditional_instances = []
	
			if count_index == 0:
				
				instances = valid_instances[0]
				
				if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
					instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
					instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
					
					x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
					
					sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
					
					sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]										
					
					month_instances = [v for j,v in enumerate(sub_valid) if all(months[int(pattern_comb[k-1])-1][v[k-1]] == months[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
					month_count = 0
					last_element = -1
					
					final_instances = []
					
					for instance in month_instances:
						if last_element == -1:
							month_count = month_count + 1
							last_element = instance[0]
							final_instances.append(instance)
						else:
							if last_element != instance[0]:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
					
					#instances[','.join(map(str,pattern_comb))] = final_instances
					#valid_instances[0] = instances
					conditional_instances = final_instances					
					counts[0] = month_count				
			else:		
				if count_index == 1:
					
					instances = valid_instances[1]
				
					if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
						instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
						instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
						
						x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
						
						sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
						
						sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
						
						month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 2 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
						month_count = 0
						last_element = -1
						
						final_instances = []
					
						for instance in month_instances:
							if last_element == -1:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
							else:
								if last_element != instance[0]:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
						
						#instances[','.join(map(str,pattern_comb))] = final_instances
						#valid_instances[1] = instances
						conditional_instances = final_instances
						counts[1] = month_count
				else:
					if count_index == 2:
						
						instances = valid_instances[2]
				
						if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
							instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
							instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
							
							x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
							
							sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
							
							sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
							
							month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 1 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
							month_count = 0
							last_element = -1
							
							final_instances = []
					
							for instance in month_instances:
								if last_element == -1:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
								else:
									if last_element != instance[0]:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
							
							#instances[','.join(map(str,pattern_comb))] = final_instances
							#valid_instances[2] = instances
							conditional_instances = final_instances
							counts[2] = month_count
					else:
						if count_index == 3:
							
							instances = valid_instances[3]
				
							if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
								instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
								instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
								
								x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
								
								sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
								
								sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
								
								month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[k-1])-1][v[k-1]] == weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
								month_count = 0
								last_element = -1
								
								final_instances = []
					
								for instance in month_instances:
									if last_element == -1:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
									else:
										if last_element != instance[0]:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
								
								#instances[','.join(map(str,pattern_comb))] = final_instances
								#valid_instances[3] = instances
								conditional_instances = final_instances
								counts[3] = month_count
						else:
							if count_index == 4:
								
								instances = valid_instances[4]
				
								if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
									instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
									instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
									
									x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
									
									sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
									
									sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
									
									month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 2*86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
									month_count = 0
									last_element = -1
									
									final_instances = []
					
									for instance in month_instances:
										if last_element == -1:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
										else:
											if last_element != instance[0]:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
									
									#instances[','.join(map(str,pattern_comb))] = final_instances
									#valid_instances[4] = instances
									conditional_instances = final_instances
									counts[4] = month_count
							else:
								if count_index == 5:
									
									instances = valid_instances[5]
				
									if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
										instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
										instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
										
										x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
										
										sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
										
										sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
										
										month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
										month_count = 0
										last_element = -1
										
										final_instances = []
					
										for instance in month_instances:
											if last_element == -1:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
											else:
												if last_element != instance[0]:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
										
										#instances[','.join(map(str,pattern_comb))] = final_instances
										#valid_instances[5] = instances
										conditional_instances = final_instances
										counts[5] = month_count
								else:
									if count_index == 6:
										
										instances = valid_instances[6]
				
										if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
											instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
											instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
											
											x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
											
											sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
											
											sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
											
											month_instances = [v for j,v in enumerate(sub_valid) if all(dates[int(pattern_comb[k-1])-1][v[k-1]] == dates[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
											month_count = 0
											last_element = -1
											
											final_instances = []
					
											for instance in month_instances:
												if last_element == -1:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
												else:
													if last_element != instance[0]:
														month_count = month_count + 1
														last_element = instance[0]
														final_instances.append(instance)
											
											#instances[','.join(map(str,pattern_comb))] = final_instances
											#valid_instances[6] = instances
											conditional_instances = final_instances
											counts[6] = month_count
										
			
			conditional_count = counts[count_index]
			
			if conditional_count > 0:
			
				probability = float(conditional_count)/pattern_combinations_count[l][m]
				
				pattern_description = ","
				
				for k in range(len(pattern_comb)):
					pattern_description = "%s%s," % (pattern_description,pattern_comb[k])
					
				pattern_condition = ","
				
				for k in range(len(pattern_comb)-1):
					pattern_condition = "%s%s," % (pattern_condition,pattern_comb[k])
				
				values = []
				values.append(user)				
				values.append(pattern_description)
				values.append(pattern_length)
				values.append(pattern_intervals[m])
				values.append(pattern_condition)
				values.append(probability)
				values.append(conditional_count)
				values.append(pattern_specificities[m])
				
				bulk_insert.append(values)
				
				values = []
				values.append(pattern_length)
				values.append(count_index)
				values.append(','.join(map(str,pattern_comb)))
				values.append(';'.join(map(To_String,conditional_instances)))
				instances_bulk.append(values)
			else:
				break		
			
		if len(bulk_insert) >= 5000:
			dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
			del bulk_insert
			bulk_insert = []
			
	dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
	dbHandler.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
		
	del bulk_insert
	del instances_bulk		
	
	
def Calculate_Probability_Length2(pattern_combinations,pattern_combinations_temporal_intervals,pattern_combinations_temporal_specificity,pattern_combinations_count,pattern_length,user,lower,upper,months,weeks,dates,timestamps,temporal_interval_description,singular_pattern_id):
	
	print "		Calculate Probabilities for user %i from pattern %s to pattern %s (%i,%i)" % (user,pattern_combinations[lower],pattern_combinations[upper-1],lower,upper)
	
	#dbHandler_pattern = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_%i" % (user))
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	fields = ["user_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	valid_instances = []
	
	for k in range(7):
		tmp = {}
		
		for sp_pattern_id in singular_pattern_id:
			tmp["%i" % (sp_pattern_id)] = range(len(timestamps[sp_pattern_id-1]))			
			
		valid_instances.append(tmp)
	
	instances_bulk = []
	bulk_insert = []
	
	for l in range(lower,upper):
		
		if len(instances_bulk) >= 10000:
			dbHandler.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			del instances_bulk
			instances_bulk = []
				
		pattern_comb = pattern_combinations[l]
		pattern_intervals = pattern_combinations_temporal_intervals[l]
		pattern_specificities = pattern_combinations_temporal_specificity[l]
		
		counts = numpy.zeros(7,int)
		
		for m in range(len(pattern_intervals)):
			count_index = temporal_interval_description.index(pattern_intervals[m])
			conditional_instances = []
	
			if count_index == 0:
				
				instances = valid_instances[0]
				
				if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
					instances_1 = instances['%s' % (pattern_comb[0])]
					instances_2 = instances['%s' % (pattern_comb[1])]
					
					sub_perm = []
					sub_perm.append(instances_1)
					sub_perm.append(instances_2)
					
					sub_perm = list(itertools.product(*sub_perm))
					sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]										
					
					month_instances = [v for j,v in enumerate(sub_valid) if all(months[int(pattern_comb[k-1])-1][v[k-1]] == months[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
					month_count = 0
					last_element = -1
					
					final_instances = []
					
					for instance in month_instances:
						if last_element == -1:
							month_count = month_count + 1
							last_element = instance[0]
							final_instances.append(instance)
						else:
							if last_element != instance[0]:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
					
					#instances[','.join(map(str,pattern_comb))] = final_instances
					#valid_instances[0] = instances
					conditional_instances = final_instances				
					counts[0] = month_count				
			else:		
				if count_index == 1:
					
					instances = valid_instances[1]
				
					if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
						instances_1 = instances['%s' % (pattern_comb[0])]
						instances_2 = instances['%s' % (pattern_comb[1])]
						
						sub_perm = []
						sub_perm.append(instances_1)
						sub_perm.append(instances_2)
						
						sub_perm = list(itertools.product(*sub_perm))
						sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
						
						month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 2 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
						month_count = 0
						last_element = -1
						
						final_instances = []
					
						for instance in month_instances:
							if last_element == -1:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
							else:
								if last_element != instance[0]:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
						
						#instances[','.join(map(str,pattern_comb))] = final_instances
						#valid_instances[1] = instances
						conditional_instances = final_instances
						counts[1] = month_count
				else:
					if count_index == 2:
						
						instances = valid_instances[2]
				
						if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
							instances_1 = instances['%s' % (pattern_comb[0])]
							instances_2 = instances['%s' % (pattern_comb[1])]
							
							sub_perm = []
							sub_perm.append(instances_1)
							sub_perm.append(instances_2)
							
							sub_perm = list(itertools.product(*sub_perm))
							sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
							
							month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 1 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
							month_count = 0
							last_element = -1
							
							final_instances = []
					
							for instance in month_instances:
								if last_element == -1:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
								else:
									if last_element != instance[0]:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
							
							#instances[','.join(map(str,pattern_comb))] = final_instances
							#valid_instances[2] = instances
							conditional_instances = final_instances
							counts[2] = month_count
					else:
						if count_index == 3:
							
							instances = valid_instances[3]
				
							if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
								instances_1 = instances['%s' % (pattern_comb[0])]
								instances_2 = instances['%s' % (pattern_comb[1])]
								
								sub_perm = []
								sub_perm.append(instances_1)
								sub_perm.append(instances_2)
								
								sub_perm = list(itertools.product(*sub_perm))
								sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
								
								month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[k-1])-1][v[k-1]] == weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
								month_count = 0
								last_element = -1
								
								final_instances = []
					
								for instance in month_instances:
									if last_element == -1:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
									else:
										if last_element != instance[0]:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
								
								#instances[','.join(map(str,pattern_comb))] = final_instances
								#valid_instances[3] = instances
								conditional_instances = final_instances
								counts[3] = month_count
						else:
							if count_index == 4:
								
								instances = valid_instances[4]
				
								if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
									instances_1 = instances['%s' % (pattern_comb[0])]
									instances_2 = instances['%s' % (pattern_comb[1])]
									
									sub_perm = []
									sub_perm.append(instances_1)
									sub_perm.append(instances_2)
									
									sub_perm = list(itertools.product(*sub_perm))
									sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
									
									month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 2*86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
									month_count = 0
									last_element = -1
									
									final_instances = []
					
									for instance in month_instances:
										if last_element == -1:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
										else:
											if last_element != instance[0]:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
									
									#instances[','.join(map(str,pattern_comb))] = final_instances
									#valid_instances[4] = instances
									conditional_instances = final_instances
									counts[4] = month_count
							else:
								if count_index == 5:
									
									instances = valid_instances[5]
				
									if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
										instances_1 = instances['%s' % (pattern_comb[0])]
										instances_2 = instances['%s' % (pattern_comb[1])]
										
										sub_perm = []
										sub_perm.append(instances_1)
										sub_perm.append(instances_2)
										
										sub_perm = list(itertools.product(*sub_perm))
										sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
										
										month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
										month_count = 0
										last_element = -1
										
										final_instances = []
					
										for instance in month_instances:
											if last_element == -1:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
											else:
												if last_element != instance[0]:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
										
										#instances[','.join(map(str,pattern_comb))] = final_instances
										#valid_instances[5] = instances
										conditional_instances = final_instances
										counts[5] = month_count
								else:
									if count_index == 6:
										
										instances = valid_instances[6]
				
										if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
											instances_1 = instances['%s' % (pattern_comb[0])]
											instances_2 = instances['%s' % (pattern_comb[1])]
											
											sub_perm = []
											sub_perm.append(instances_1)
											sub_perm.append(instances_2)
											
											sub_perm = list(itertools.product(*sub_perm))
											sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
											
											month_instances = [v for j,v in enumerate(sub_valid) if all(dates[int(pattern_comb[k-1])-1][v[k-1]] == dates[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
											month_count = 0
											last_element = -1
											
											final_instances = []
					
											for instance in month_instances:
												if last_element == -1:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
												else:
													if last_element != instance[0]:
														month_count = month_count + 1
														last_element = instance[0]
														final_instances.append(instance)
											
											#instances[','.join(map(str,pattern_comb))] = final_instances
											#valid_instances[6] = instances
											conditional_instances = final_instances
											counts[6] = month_count
										
			
			conditional_count = counts[count_index]
			
			if conditional_count > 0:
			
				probability = float(conditional_count)/pattern_combinations_count[l][m]
				
				pattern_description = ","
				
				for k in range(len(pattern_comb)):
					pattern_description = "%s%s," % (pattern_description,pattern_comb[k])
					
				pattern_condition = ","
				
				for k in range(len(pattern_comb)-1):
					pattern_condition = "%s%s," % (pattern_condition,pattern_comb[k])
				
				values = []
				values.append(user)				
				values.append(pattern_description)
				values.append(pattern_length)
				values.append(pattern_intervals[m])
				values.append(pattern_condition)
				values.append(probability)
				values.append(conditional_count)
				values.append(pattern_specificities[m])
				
				bulk_insert.append(values)
				
				values = []
				values.append(pattern_length)
				values.append(count_index)
				values.append(','.join(map(str,pattern_comb)))
				values.append(';'.join(map(To_String,conditional_instances)))
				instances_bulk.append(values)
			else:
				break		
			
		if len(bulk_insert) >= 10000:
			dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
			del bulk_insert
			bulk_insert = []
			
	dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
	dbHandler.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			
	del bulk_insert
	del instances_bulk
	
# ================================= Begin of Support Methods =================================

def Get_Interval_Name(index):
	
	if index == 0:
		return "same"
	
	if index == 1:
		return "adjacent_1"
	
	if index > 1 and index % 2 == 0:
		return "at_adjacent_%i" % (index/2 + 1)
	
	if index > 1 and index % 2 != 0:
		return "within_adjacent_%i" % ((index-1)/2 + 1)

def Probability_Count(doy,search_list,search_element):
	
	last_doy = -1
	search_count = 0
	
	for i in range(len(doy)):
		
		if last_doy != doy[i]:						
			if search_list[i] == search_element:
				search_count = search_count + 1
			
		last_doy = doy[i]
		
	return search_count
	
def Find_Elements(search_list,element):
	
	search_indices = []
	
	for i in range(len(search_list)):
		if search_list[i] == element:
			search_indices.append(i)
			
	return search_indices
	
def Get_Member_List(event_ids,reps):
	
	output = ","
	
	for i in range(len(reps)-1):
		output = "%s%i," % (output,event_ids[reps[i]])
		
	output = "%s%i," % (output,event_ids[reps[-1]])
	
	return output


def Get_Cluster_ID(activity,location,cluster_index):
	
	if activity == 'Too Hot':
		return "TH_%i_%i" % (location,cluster_index)

	if activity == 'Too Cold':
		return "TC_%i_%i" % (location,cluster_index)
	
	if activity == 'Comfortable':
		return "C_%i_%i" % (location,cluster_index)
	
	if activity == 'Cancel':
		return "CI_%i_%i" % (location,cluster_index)
	
	if activity == 'First Interaction':
		return "FI_%i_%i" % (location,cluster_index)
	
	if activity == 'Last Interaction':
		return "LI_%i_%i" % (location,cluster_index)
	

def Find_Best_Clustering(user,activity):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	result = dbHandler.select("SELECT outer_temporal_threshold,inner_temporal_threshold,(number_short_clusters/number_clusters) as cluster_length_coef FROM Pattern_Analysis WHERE activity = '%s' AND user_id = %i ORDER BY dunn_index DESC LIMIT 5" % (activity,user))
	
	outer_thresholds = []
	inner_thresholds = []
	cluster_coef = []
	
	for row in result:
		outer_thresholds.append(int(row[0]))
		inner_thresholds.append(int(row[1]))
		cluster_coef.append(float(row[2]))
		
	choice = random.randint(0,4)
	#lowest = numpy.argmin(numpy.array(cluster_coef))
	
	return outer_thresholds[choice]/5,inner_thresholds[choice]/5
		

def Calculate_Cluster_Density(means):
	
	# Calculates distance of all points to the mean
	
	distances = []
	cluster_mean = numpy.mean(numpy.array(means))
	
	for i in range(len(means)):		
		distances.append(math.fabs(means[i]-cluster_mean))
			
	
	return float(numpy.sum(numpy.array(distances)))/float(len(means))
	
def Find_Nearest(array,value):
    idx = (numpy.abs(array-value)).argmin()
    return idx

def Calculate_Pattern_Coverage(user,length):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	distinct_count = []
	
	result = dbHandler.select("SELECT DISTINCT count FROM Pattern_Base WHERE user_id = %i AND pattern_length = %i ORDER BY count DESC" % (user,length))
	
	for row in result:
		distinct_count.append(int(row[0]))
		
	distinct_patterns = []
	
	for k in range(distinct_count[0]):
		distinct_patterns.append([])
	
	result = dbHandler.select("SELECT count,pattern_members FROM Pattern_Base WHERE user_id = %i AND pattern_length = %i" % (user,length))
	
	for row in result:
		distinct_patterns[int(row[0])-1].append(row[1][1:-1])		
			
	distinct_patterns_tmp = []
	
	for pattern_collection in distinct_patterns:
		tmp = list(set(pattern_collection))
		
		sp_tmp = []
		
		for pattern in tmp:
			members = pattern.split(",")
			
			for k in range(len(members)):
				sp_tmp.append(int(members[k]))		
		
		distinct_patterns_tmp.append(list(set(sp_tmp)))
		
	result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	max_sp_id = 0
	
	for row in result:
		max_sp_id = int(row[0])
	
	singular_patterns = []
	
	for k in range(max_sp_id):
		singular_patterns.append([])
	
	result = dbHandler.select("SELECT pattern_id,members FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (user))
	
	for row in result:		
		singular_patterns[int(row[0])-1] = row[1][1:-1].split(",")
		
	event_list = []
		
	for patterns in distinct_patterns_tmp:
		
		events = []
		
		for k in range(len(patterns)):
			events_tmp = singular_patterns[patterns[k]-1]
			
			for l in range(len(events_tmp)):
				events.append(events_tmp[l])
				
		event_list.append(events)
		
	result = dbHandler.select("SELECT max(event_id) FROM Events_GHC_final WHERE user_id = %i" % (user))
	
	max_event_id = 0
	
	for row in result:
		max_event_id = int(row[0])
		
	coverage = []
	
	for events in event_list:
		coverage.append(float(len(list(set(events))))/float(max_event_id))
		
	print coverage
	
	total_list = []
	
	for k in range(3,len(event_list)):
		for l in range(len(event_list[k])):
			total_list.append(event_list[k][l])
			
	print float(len(list(set(total_list))))/float(max_event_id)

def Clean_Complaints_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics")
	dbHandler_1 = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	bulk_insert = []
	
	result = dbHandler.select("SELECT * FROM complaints")
	
	for row in result:
		date_time = row[3].split(" ")
		
		tmp_row = list(row)
		
		tmp_row.pop(0)
		tmp_row[-2] = date_time[0]
		tmp_row[-1] = date_time[1].split(".")[0]
		
		bulk_insert.append(tmp_row)
		
		if len(bulk_insert) >= 5000:
			dbHandler_1.insert_bulk("complaints",['user_id','username','created_at','type','temp','location_id','blast_action','date','time'],bulk_insert)
			bulk_insert = []
			
	dbHandler_1.insert_bulk("complaints",['user_id','username','created_at','type','temp','location_id','blast_action','date','time'],bulk_insert)
	
def Clean_Events_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	zones = []
	
	result = dbHandler.select("SELECT location_id,count(id) FROM complaints GROUP BY location_id ORDER BY count(id) DESC")
	
	for row in result:
		zones.append(int(row[0]))
		
	for zone in zones:
		result = dbHandler.select("SELECT activity,avg(current_temperature) FROM Events_BR_final WHERE zone_id = %i AND current_temperature != 0 GROUP BY activity" % (zone))
		
		activities = []
		temperature = []
		
		for row in result:
			activities.append(row[0])
			temperature.append(float(row[1]))
			
		for k in range(len(activities)):
			dbHandler.update("UPDATE Events_BR_final SET current_temperature = %f WHERE current_temperature = 0 AND activity = '%s' AND zone_id = %i" % (temperature[k],activities[k],zone))
	
def Load_Valid_Instances(user):
	
	pickle_file = open("valid_instances_%i_2.pkl" % (user), "r")
	valid_instances = pickle.load(pickle_file)
	pickle_file.close()
	
	instances_1 = valid_instances[6][','.join(map(str,[1,33]))]
	instances_2 = valid_instances[6][','.join(map(str,[1,99]))]
	
	print instances_1
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	fields = ['pattern_length','temporal_interval','key','value']
	
	value = []
	value.append(2)
	value.append(6)
	value.append(','.join(map(str,[1,33])))
	value.append(';'.join(map(To_String,instances_1)))
	
	dbHandler.insert("Valid_Instances",fields,value)

def To_String(inst):
	
	output = ""
	
	for i in range(len(inst)-1):
		output = "%s%s," % (output,inst[i])
		
	output = "%s%s" % (output,inst[-1])
	
	return output

def Get_Valid_Instances(pattern_length,user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	result = dbHandler.select("SELECT * FROM Valid_Instances_%i WHERE pattern_length = %i" % (user,pattern_length))
	
	valid_instances = []
	
	for k in range(7):
		valid_instances.append({})
		
	for row in result:
		valid_instances[int(row[2])][row[3]] = [map(int,v.split(",")) for j,v in enumerate(row[4].split(";"))]
		
	return valid_instances	
	

# ================================= End of Support Methods =================================

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
			
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Building_Robotics_Pattern_Base")
	
	result = dbHandler.select("SELECT location_id,count(id) FROM complaints WHERE user_id != 177 GROUP BY location_id ORDER BY count(id) DESC")
	
	zones = []
	
	for row in result:		
		#Extract_Singular_Patterns_BR(int(row[0]))
		zones.append(int(row[0]))
		
	#Map_Singular_Patterns_BR()
	num_threads = 3
	pool = Pool(num_threads)
		
	for zone in zones:
		Extract_Conditional_Patterns(zone,2,pool)
	
	#Extract_Conditional_Patterns(177,2)
	#Load_Valid_Instances(177)
	