#!/usr/bin/env python

import sys

for line in sys.stdin:
	try:
		line = line.strip()

		splits = line.split(',')

		custID = "-1"
		transID = "-1"
		name = "-1"
		countryCode = "-1"

		if splits[1].isdigit():
			custID = splits[1]
				
			transID = splits[0]

		else:
			#if countryCode == '5':
			custID = splits[0]
			name = splits[1]
			countryCode = splits[3]
				#send_trans = 1
		
		print '%d,%s,%s,%s' % (int(custID), transID, name, countryCode)
	except:
		pass



