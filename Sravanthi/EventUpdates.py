import os
import logging
import pandas as pd
from typing import list, dict
import sqlite3
import xml.etree.ElementTree as ElementTree

class RO:
	def __init__(self,order_id,date_time,status,cost,technician,parts):
		self.order_id=order_id
		self.date_time=date_time
		self.status=status
		self.cost=cost
		self.technician=technician
		self.parts=parts

def read_files_from_dir(dir_path: str, prefix:str) -> List[str]:
	files=[]
	for file_name in os.listdir(dir_path):
		if file_name.startswith(prefix) and file_name.endswith('.xml'):
			with open(os.path.join(dir_path,file_name),'r')as file:
				files.append(file.read())
	return files 
	
def parse_xml(files: List[str]) -> pd.DataFrame:
	data={
	'order_id': [],
	'date_time': [],
	'status': [],
	'cost': [],
	'technician': [],
	'parts': []
	}
	for file_content in files:
		root= ET.fromstring(file_content)
		for event in root.findall('event'):
			order_id = event.find('order_id').text
			date_time = event.find('date_time').text
			status = event.find('status').text
			cost =float(event.find('cost').text)
			technician = event.find('repair_details/technician').text
			parts = [(part.attrib['name'],int(part.attrib['quantity'])) for part in event.findall('repair_details/repair_parts/part')]
			data['order_id'].append(order_id)
			data['date_time'].append(date_time)
			data['status'].append(status)
			data['cost'].append(cost)
			data['technician'].append(technician)
			data['parts'].append(parts)
	df = pd.DataFrame(data)
	df['date_time'] = pd.to_datetime(df['date_time'])
	return df 

def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
	windows = {}
	for name,group in data.groupby(pd.Grouper(key='date_time', freq=window)):
		windows[str(name)] = group.sort_values(by='date_time').iloc[-1:]
	return windows

def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
	ROs =[]
	for window, df in data.items():
		for _, row in df.iterrows():
			ro = RO(
				order_id=row['order_id'],
				date_time=row['date_time'],
				status=row['status'],
				cost=row['cost'],
				technician=row['technician'],
				parts=row['parts']
				)
			ROs.append(ro)
	return ROs
	
def database_write(ROs:List[RO], db_path: str):
	conn = sqlite3.connect(db_path)
	c = conn.cursor()
	c.execute('''Create table if not exists repair_orders
		           (order_id TEXT, date_time TEXT, status TEXT, cost REAL, technician TEXT, parts TEXT)''')
	for ro in ROs:
		parts_str = ", ".join([f"{part[0]} ({part[1]})" for part in ro.parts])
		c.execute("Insert into repair_orders values (?, ?, ?, ?, ?, ?)",
					(ro.order_id, str(ro.date_time),ro.status, ro.cost, ro.technician, parts_str))
	conn.commit()
	conn.close()

def setup_logging():
	logging.basicConfig(filename= 'pipeline.log', level= logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')

def main(dir_path: str, prefix: str, window: str, db_path: str):
	setup_logging()
	logging.info("Starting Pipeline----->")
	logging.info(f"Reading XML files from directory:{dir_path}")
	files = read_files_from_dir(dir_path, prefix)
	logging.info(f"Found {len(files)} XML files.")
	logging.info("Parsing XML files----->")
	df= parse_xml(files)
	logging.info("Windowing Data----->")
	windows = window_by_datetime(df, window)
	logging.info("Processing data to RO format------>")
	ROs = process_to_RO(ROs, db_path)
	logging.info("Writing output to Database------>")
	database_write(ROs, db_path)
	logging.info("Pipeline Successfully Completed.")	

if __name__ == "__main__":
	dir_path = "path/to/xml_files_directories"
	prefix = "event_updates"
	window = "1D"
	db_path="path/to/database.db"
	main(dir_path, prefix, window, db_path)




