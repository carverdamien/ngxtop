import sqlite3
import csv
import datetime

def main(args):
	# input
	fn_database = 'ngxtop.db'
	# outputs
	fn_workset  = 'workset.csv'
	fn_scenario = 'scenario.csv'
	conn = sqlite3.connect(fn_database)
	cursor = conn.cursor()
	# Build map filename to anonym filename
	map_filename = build_map_filename(cursor)
	map_addr = build_map_addr(cursor)
	output_workset(fn_workset,sorted(map_filename.values(),key=lambda x:x[0]))
	output_scenario(fn_scenario,cursor,map_filename,map_addr)
	conn.close()
	pass

def build_map_addr(cursor):
	m = {}
	i = 0
	cursor.execute("select distinct remote_addr from log")
	for row in cursor:
		m[row[0]] = i
		i = i + 1
	return m

def build_map_filename(cursor):
	m = {}
	i = 0	
	cursor.execute("select filename, size from (select request_path as filename, max(bytes_sent) as size from log where bytes_sent > 4096 group by request_path) order by size")
	for row in cursor:
		filename = row[0]
		size = row[1]
		m[filename] = (i,size)
		i = i + 1
		pass
	return m

def output_workset(fn_workset,workset):
	with open(fn_workset,'w') as cvs_file:
		writer = csv.writer(cvs_file, delimiter=',')
		writer.writerow(['data','bytes'])
		for row in workset: writer.writerow(row)
		
start_time = None

def convert_time(strtime):
	global start_time
	time = datetime.datetime.strptime(strtime.split(" ")[0],'%d/%b/%Y:%H:%M:%S')
	if start_time == None:
		start_time = time
		return 0
	return int((time - start_time).total_seconds())

def output_scenario(fn_scenario,cursor,map_filename,map_addr):
	csvheader = ['time','data','bytes','addr']
	header = ['time_local', 'request_path', 'bytes_sent', 'remote_addr']
	cursor.execute("select %s from log where bytes_sent > 4096 order by time_local" % (" ,".join(header)))
	db_header = [d[0] for d in cursor.description]
	for h in header:
		if not h in db_header:
			raise Exception('Missing header')
	with open(fn_scenario,'w') as cvs_file:
		writer = csv.writer(cvs_file, delimiter=',')
		writer.writerow(csvheader)
		for row in cursor:
			(strtime,filename,bytes,remote_addr) = row
			time = convert_time(strtime)
			data = map_filename[str(filename)][0]
			addr = map_addr[remote_addr]
			writer.writerow([time,data,bytes,addr])
		pass

if __name__ == '__main__':
	import sys
	main(sys.argv)
