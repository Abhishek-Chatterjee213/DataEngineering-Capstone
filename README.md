1. Project Summary
 `This project aims to be able to answers questions on US immigration such as what are the most popular cities for immigration, what is the gender distribution of the immigrants, what is the visa type distribution of the immigrants, what is the average age per immigrant and what is the average temperature per month per city. We extract data from 3 different sources, the I94 immigration dataset of 2016, city temperature data from Kaggle and US city demographic data from OpenSoft. We design 4 dimension tables: Cities, immigrants, monthly average city temperature and time, and 1 fact table: Immigration. We use Spark for ETL jobs and store the results in parquet for downstream analysis.`

2. Data sources
    
    * I94 Immigration Data: `comes from the U.S. National Tourism and Trade Office and contains various statistics on international visitor arrival in USA and comes from the US National Tourism and Trade Office. The dataset contains data from 2016.`
    * World Temperature Data: `comes from Kaggle and contains average weather temperatures by city.`
    * U.S. City Demographic Data: `comes from OpenSoft and contains information about the demographics of all US cities such as average age, male and female population.`
    
3. Data Dictionary
    
    3.1 ER Diagram - 
    
      ![Alt](img/erd.PNG "ERD")

    3.2 Dimension Tables
        
        * dim_demos - This holds data about a city e.g - city_name, state, population details (median_age, percentage of male & female etc)
        
           `city_code: city port code --> primary key
            state_code: state code of the city --> partition key
            median_age: median age of the city
            pct_male_pop: city's male population in percentage
            pct_female_pop: city's female population in percentage
            pct_veterans: city's veteran population in percentage
            pct_foreign_born: city's foreign born population in percentage
            pct_native_american: city's native american population in percentage
            pct_asian: city's asian population in percentage
            pct_black: city's black population in percentage
            pct_hispanic_or_latino: city's hispanic or latino population in percentage
            pct_white: city's white population in percentage
            total_pop: city's total population
            lat: latitude of the city
            long: longitude of the city`
            
        * dim_monthly_temperature_by_city - This holds temparature data for a city.
        
            `city_code: city port code --> Foreign Key References dim_cities
            year: year
            month: month 
            avg_temperature: average temperature in city for given month`
            
        * dim_time - Holds details of a day.
        
            `date: date --> primary key
            dayofweek: day of the week
            weekofyear: week of year
            month: month`
        
        * dim_country - Holds country mapping
          
            `country_code : country code --> Primary Key
            country_name: Country Name`
      
       * dim_states - Holds state_code to state mapping
    
          `state_code : Stqte Code --> Primary Key
           state_name : State Name`
       
       * dim_city - Holds city_code to city_name mapping
   
           `city_code : City Code --> Primary Key
           city_name : City Name`
       
       * dim_visa - Holds Visa type to visa description mapping
       
           `visa_code: Visa Code --> Primary Key
           visa_desc: Visa Description`

    3.3 Fact Table
        * fact_immigration - Fact table where the foreign keys are taken from different dimenssion tables and stores the count of immigrants entered in us.
        
         `immigrant_id: id --> Primary Key
          state_code: --> state code of arrival city Foreign Key dim_state
          city_code: city port code of arrival city --> Foreign Key REFERENCES dim_city
          resident_country: Country of residence --> Foreign Key References dim_city
          date: date of arrival --> Foreign Key REFERENCES dim_time
          age : Age
          gender: Gender
          visa_type: Visa Type
          count: count of immigrant's entries into the US`
            
4. Tools, Technology choice & Alternatives

    Spark is chosen here as the data processing framework as it has capability of processing large distributed data efficiently. Apart from that spark has a wide variety of read, write and data processing transformations available for different file formats and filesystems.

    There are also considerations in terms of scaling existing solution.

    1. If the data was increased by 100x: 

        `We can cosider having large scale emr set up or setting up spark on Kubernetes so that we can make use of vertical and horizontal scalings.`

        `Also, we might consider using Cassandra as a temprary storage which is higly available and provides pretty fast write speed. From there we can load our DW dbs as required.`

    2. If the data populates a dashboard that must be updated on a daily basis by 7am every day: 

        `We can consider using Airflow or Kubeflow to schedule and automate the data pipeline jobs. We can set up prometheus and grafana to do automatic alerting based on specific conditions as well as retrying the failed jobs.`

    3. If the database needed to be accessed by 100+ people: 

        `We can consider hosting our solution in production scale data warehouse in the cloud, with larger capacity to serve more users, and workload management to ensure equitable usage of resources across users.`
    4. If there is a chance the data structure can change in future :
        ` We can use a data cataloging tool like AWS glue to manage and maintain the schema.`
    
    5. If Raw & Unstructured Data access is needed:

        `Desigining a data lake with AWS Glue can be a good option along with Querying via AWS Athena / Apache Drill.`

5. Next Steps

    * Convert the ipynb file to .py file.
    * Add functionality for loading the tables into Redshift DB instead of creating parquet files.
    * Schedule it using Airflow in a EC2 / EMR cluster.
    * Use quicksight / power bi to visualize data.