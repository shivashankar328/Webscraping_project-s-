
# Nasdaq results

Nasdaq stocks hostorical data search 



## Command to run the code 

scrapy runspider nasdaqresults.py -a searchkey=amzn -o amazon.csv

#  company codes
amazon = amzn
apple = aapl
microsoft = mstf

replace the searchkey with user required company code.it will collect the last 5 years histrocial data.

# mysql connection
in piplelines.py mysql connection is made to upload the data to mysql. replace the database login details with your user details.