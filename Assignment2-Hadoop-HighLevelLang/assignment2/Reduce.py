#!/usr/bin/env python

import sys
current_count= 0
currentID = None
start = True
country_code = None
for line in sys.stdin:	

	line = line.strip()

	splits = line.split(',')
	custId = splits[0]
	#splits[1] = splits[1][1:]
	#Get the initial customer id
	if start:
		currentID = custId
		start = False

	if splits[1] == "-1":
		currentName = splits[2]
		country_code = splits[3]
	else:
		current_count += 1	

	if currentID != custId:
		if country_code == '5':
			print '%d,%s,%d' % (int(currentID), currentName, current_count)
		current_count = 0
		currentID = custId
		currentName = None	
		
print '%s,%s,%d' % (currentID, currentName, current_count)

		
		



