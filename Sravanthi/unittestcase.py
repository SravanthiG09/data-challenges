import unitest
import  parse_xml

class ParseXMLTest(unitest.Testcase):
	def parse_xml_test(self):
		sample_xml = """
		<event>
    		<order_id>101</order_id>
    		<date_time>2023-08-10T10:00:00</date_time>
    		<status>In Progress</status>
    		<cost>50.25</cost>
    		<repair_details>
        		<technician>Jane Smith</technician>
        		<repair_parts>
           	 		<part name="Air Filter" quantity="1"/>
        		</repair_parts>
    		</repair_details>
		</event>
		"""

		expected_output = {
		'order_id' : ['101'],
		'date_time' : ['2023-08-10T10:00:00'],
		'status': ['In Progress'],
		'cost' : [50.25],
		'technician':['Jane Smith'],
		'parts': [[('Air Filter', 1)]]
		}

		parsed_df= parse_xml([sample_xml])
		self.assertEqual(parsed_df.to_dict(), expected_output)

if __name__ == '__main__':
	unittest.main()
