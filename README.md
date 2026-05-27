# cart-abandonment-analysis
This is an end-to-end data project to analyze the cart abandonment in an e-commerce business. Goal is to identify the unique carts belong to the customers that don't have subsequent successful checkout events and aggregate them to find the cart value per customer. If the cart value is up-to 100$, then it's bucketed under 5% promotional category. If cart value is more than 100$, then 10% promotional category is applied. 
These segmented customers ideally receive promotional codes depending on their bucket to enable customers to place order instead of abandoning their cart. This is not implemented in this project due to practical difficulty of sending actual emails to code generated sample email addresses.
The project also analyses the abandoned carts and derives insights such as which region has most abandoned carts, what date has seen more cart abandonment (Trend), what are the some of the most reccuring errors (reasons for cart abandonment) users see etc.


Tech-stack used:
. AWS Lambda function for sample data generation using faker.
. EC2 instance with Ubuntu OS as infrastructure to host the airflow platform
. Airflow is installed and managed on EC2 instance using Docker and Docker compose
. AWS S3 buckets are used to store the data in medallion way. One path each for raw data, aggregated and segmented data and analytical data.

