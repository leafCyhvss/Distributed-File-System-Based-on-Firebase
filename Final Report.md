# Final Report

Yucheng Yang 2333448896
Junhui Yang 9165660545
Carra Hamner 7561895800

## **Introduction**

With the drastic changes in the economic situation in recent years, the selling price of used cars has become more variable. In this project, by building the distributed file system and implementing the Mapreduce algorithm, we analyzed the prices of Ford, Audi and Toyota used cars in the market in recent years and the related factors affecting their prices.

First of all, we build two independant emulated distributed file systems (EDFS). Both EDFS is base on Firebase remote nosql database. However, EDFS1 is implemented with Firebase’s Python SDK, i.e. `python-firebase` package,  while EDFS2 is built with RESTFul query directly to the online database.

Second, we implemented the map reduce algorithm according to the principles discussed in class. We designed several `mapper` and `reducer` which will contribute to commiting search and analytics functions on our EDFSs.

Last,  based on the characteristics of the data set, we analyzed the relationship between some factors and the selling prices of Ford, Audi and Toyota in the second-hand car market, and drew pictures to show the analysis results.

## **Explanations of Our Original Dataset**

These three dataset, i.e. `ford.csv`, `audi.csv`, and `toyota.csv`, are representing UK used car’ selling price in recent years and the characteristics of the cars themselves. Each of the dataset contains nine columns: 

1. Model: the car model of different brands of cars
2. Year: the selling time of the used cars
3. Price: the selling price of the used cars
4. Transmission: Auto, Semi-Auto, Manual or other
5. Mileage: the number of miles covered by the vehicle
6. FuelType: Petrol or diesel or other
7. Tax: Tax of selling deals
8. mpg: Miles per gallon
9. Engine size: Size of the engine

 And there are about 10,000 records in each CSV file.

## **Project design**

### **Part 1: EDFS**

For building an emulated distributed file system (EDFS), we use Firebase-based emulation to store csv files as json. Type of files stored in the file system is limited to `CSV` files only. EDFS1, as shown in the project codes, is implemented on `python-firebase` SDK. This method can interact with remote servers quickly and efficiently. EDFS2 communicates with the remote Firebase server with direct RESTFul queries, just as the same as the queries introduced in the lectures before the first midterm. The advantages and disadvantages of using both approaches will be discussed in a later section.

Apart from storing the actual data from csv files into Firebase, both EDFS store the metadata about the file system, which contains the implicit relationship between files and Datanodes.

The EDFSs fully support the commands listed in the project guideline. EDFS2 supports user to navigate starting from the root of file system `/`. And EDFS1 only supports users to travel from level 1 directories under the root directory to the deeper directories. All of the paths begins with a `/` in both EDFSs.

In terms of coding, EDFS1 provides functions directly while EDFS2 wraps everything into a python class `EDFSURL`.

### **Part 2: Partition-based map and reduce **

We combined partition-based map and reduce functions with search and analytics functions in part 3.

### **Part 3: App for searching and analyzing**

We used the Django framework for the web browser-based application required for a three-person team. We decided to use it because it is the fastest framework for developing websites. In addition, it uses Python, HTML, and CSS for coding. Our team members who are responsible for this application are both familiar with Python and had some experience with HTML With the aid of Bootstrap, a framework of CSS, we only need to write Python codes and HTML templates, ensuring a quick development process. 

EDFS1 and EDFS2 share the same HTML templates located in the templates folder. layout.html includes content in the static folder for styling and pictures. All other HTML templates except analytics.html and report.html for our analytics explanation and final report extend layout.html, making each EDFS project's HTML/CSS skeleton roughly the same. In viewS.py and viewF.py, we use x-request.html files to process GET requests from forms to gather data from each EDFS’s command, and x-result.html to process POST requests for results gathered for CAT, GetPartitionLocations, and ReadPartition. For the ls command, we used x-ls-post.html to handle the form. We render all other commands’ results with x-ls.html. POST data are sent via key-value pairs and rendered accordingly regarding their purposes. 

## Analyze

#### The annual transmission output of the three businesses and the overall number of used vehicles:

<img src="./Final%20Report.assets/13391669697059_.pic-9697115.jpg" alt="13391669697059_.pic" style="zoom: 67%;" />

<img src="./Final%20Report.assets/13411669697060_.pic-9697160.jpg" alt="13411669697060_.pic" style="zoom: 67%;" />

<img src="./Final%20Report.assets/13431669697061_.pic.jpg" alt="13431669697061_.pic" style="zoom: 67%;" />

According to the three graphs above, the production of Audi's automated and semi-automated vehicles climbed dramatically in 2019 while the production of manual vehicles decreased. Toyota's manual vehicle production has been expanding year after year, and their auto cars are also being produced in large numbers, but their manual transmission is always at the top. Ford produces about 90% of its vehicles with a manual gearbox. Only 10% of Ford vehicles are equipped with gearboxes other than the manual. Ford and Toyota have shown little interest in semi-autonomous vehicles. Audi's Semi-Auto production has increased year after year, and in  2019 it surpassed the Manual Transmission.  Based on the foregoing, we believe that Audi outperforms Ford and Toyota in auto and semi auto transmission vehicles. On the other hand, Toyota and Ford are competing in the manual gearbox market.  We can conclude that people's demanding of used cars increased in recent years.

#### The used car price trend in recent years:

We will see that the company's prices have been increasing linearly or exponentially for all of  their used vehicles. The prices have climbed linearly every year at a positive rate for all three companies, as seen by  the red line in each of the three graphs that have been presented above. For Audi, the price rise  did not begin until after the year 2010, for Ford it was somewhere between 2007 and 2010,  while for Toyota, the price increase did not begin until after the year 2005. We believe this trends somehow reflect the trend of inflation.

## **Learning Experience**





## **Conclusion**

After this project, we have deep learning about how to implement an emulation-based system for distributed file storage and parallel computation and how to deal with big data. We also learned more about the web development framework Django and gathered some hands-on experience with it. We now know how to set up a Django project with sub-projects, render pages with POST data (learned that one of the best ways is to use key-value pairs), how to redirect to another page, and how use Bootstrap with Django. Besides, we also learned how to do data analysis by using Python. 



## **Links**