# Welcome to DE-Terrific-Totes-Team-8
## Team Trent's Northcoders project 
## June 2025

### Overview 
The project is an `ETL` task, hosted on Amazon Web Services (AWS) involving three lambda functions that perform:\
• `Extraction` of 11 tables of sales data from an OTP database Totesys into an S3 ingestion bucket as JSON files.\
• `Transformation` of 6 of the 11 original tables to create 7 related tables in a star schema format. These are saved in a separate S3 processed bucket as parquet files.\
• `Loading` of data from the parquet files into an OLAP database Warehouse.

![Screenshot from 2025-06-13 09-06-46](https://github.com/user-attachments/assets/46eb4e3f-5105-4248-9355-210f6c8fb441)


After the initial extraction of data, the process is triggered every 20 minutes to collect latest updates to any of the original tables. If there are any new data, these are saved into the Warehouse. If an error occurs, Cloudwatch email alerts are triggered.

We used github actions for automating our CI/CD. We used a makefile to create the virtual environment, install dependencies and run tests. 

This project was written in <ins>Python 3.13 </ins> using modules: 
- pg8000 for database connections
- boto3 for AWS connections
- pandas and awswrangler to transform tables into dataframes and output files
- pytest for unit and integration testing
- moto for testing of functions using AWS clients and resources
- bandit, pip-audit, flake8 and black were used to make security and pep-8 compliance checks  

<ins>Terraform</ins> was used to set up infrastructure on the AWS cloud.

<ins>SQL</ins> was used within or outside pg8000 to query and write to the databases.

<ins>Tableau</ins> was used for data visualisation.

Apart from the technical work, the team applied Agile methodology. A Trello kanban board was used to organise tasks. Twice daily scrums were held with a rotating scrum leader.

![image](https://github.com/user-attachments/assets/5faecfed-3bb3-4e46-8e64-841b14f5de99)


### Setup Instructions 
#### Prerequisites 

You will need Python 3, an AWS account and an IAM user to deploy the terraform infrastructure. 

- Get started by forking and cloning this repository. 
- In the terminal, run `make requirements` to create a virtual environment, install dependencies and run checks.

#### Configure AWS Credentials
- Configure AWS Credentials with `aws configure`. Enter your `AWS Access Key ID` and `Secret Access Key`.

#### Provision AWS Resources with Terraform
- Navigate to the `terraform` directory. In the terminal, run the following:
```
terraform init 
```
*Initialises the working directory, containing Terraform configuration files.*   
```
terraform plan 
```
*Creates a preview of the changes Terraform will make.*
```
terraform apply 
```
*Performs the changes shown in the plan.* 

- To remove the infrastructure, run:
```
terraform destroy 
```
*Destroys the infrastructure.*

### Collaborators 

[Ethan Yee](https://github.com/EthanYee9)  
[Annette Alcasabas](https://github.com/annette-alca)\
[Leighton Jones](https://github.com/LeightonJones)\
[Eashin Matubber](https://github.com/eeashin)\
[Sonika Jha](https://github.com/s-onika) 
