#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import math
import array
import sys
import os
import Database_Handler

# Main Program
if __name__ == "__main__":
	
	dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Building_Robotics")
	
	fields = ['room','date','time','airflow_1','airflow_2','temperature_comfy_high','temperature_comfy_low','setpoint_high','setpoint_low','temperature']
	
	root_dir = "/Users/chrisi00/GIT/sZjQjswG2h9E2TOp9lMmPe/"
	
	for file in os.listdir("%sData/" % (root_dir)):
		if file.endswith(".csv"):
			
			data_file = open("%sData/%s" %(root_dir,file),"r")
			
			try:
				first_line = True
				
				insert_bulk = []
			
				for line in data_file:
					if len(insert_bulk) >= 5000:
						dbHandler.insert_bulk("VAV_Data",fields,insert_bulk)
						insert_bulk = []
					
					if first_line:
						first_line = False
					else:
						data = line.split(",")
						date_time = data[0].split(" ")
						
						values = []
						values.append(file.split(".")[0])
						values.append(date_time[0])
						values.append(date_time[1])
						
						for i in range(1,len(data)):
							values.append(data[i])
						
						insert_bulk.append(values)
						
			finally:
				data_file.close()