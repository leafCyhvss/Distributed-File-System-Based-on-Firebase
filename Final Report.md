# Final Report

Yucheng Yang 2333448896
Junhui Yang 9165660545
Carra Hamner 7561895800

## **Introduction**

First, we will explain our original dataset, then followed by our project design which contains three parts: EDFS, Partition-based map and reduce(PMR) and App for searching and analyzing. In addition, we will talk about our prediction and analysis based on the data we use. Then we will discuss the learning experience. Last but not least, we will conclude our project.

## **Explanations of Our Original Dataset**

We choose the UK used car dataset from Kaggle in order to analyze the used car price trend and the production of used cars. To achieve that, we have three CSV datasets, Audi, Ford, and Toyota. Each of them contains nine columns: 1. Model, 2. Year, 3. Price, 4. Transmission, 5. Mileage, 6. FuelType, 7. Tax, 8. mpg, and 9. Engine size. And there are about 10,000 records in each CSV file.

## **Project design**

### **Part 1: EDFS**

For Building an emulated distributed file system (EDFS), we use Firebase-based emulation to store JSON files. The database stores the actual data of the file as a data node, while the database stores the metadata as a name node. The EDFSs fully support the commands which are listed in the project guideline. By implementing two EDFSs, Junhui and Yucheng choose different approaches to connect to firebase: SDK and query. EDFS Project folder specifies the project configuration of the project. The project includes two different sub-projects, EDFS1 and EDFS2. Each of the EDFS folders functions as smaller projects including EDFS implementations with the aid of the EDFS Project. 

### **Part 2: Partition-based map and reduce **

For implementing map-reduce on data stores on EDFS, Junhui performed the map and reduce function by himself. Junhui implemented search, map, reduce and analytic functions and Yucheng implemented analytic functions. These functions are built for Part 3 which is searching and analyzing in application development. 

### **Part 3: App for searching and analyzing**

We used the Django framework for the web browser-based application required for a three-person team. We decided to use it because it is the fastest framework for developing websites. In addition, it uses Python, HTML, and CSS for coding. Our team members who are responsible for this application are both familiar with Python and had some experience with HTML With the aid of Bootstrap, a framework of CSS, we only need to write Python codes and HTML templates, ensuring a quick development process. 

EDFS1 and EDFS2 share the same HTML templates located in the templates folder. layout.html includes content in the static folder for styling and pictures. All other HTML templates except analytics.html and report.html for our analytics explanation and final report extend layout.html, making each EDFS project's HTML/CSS skeleton roughly the same. In viewS.py and viewF.py, we use x-request.html files to process GET requests from forms to gather data from each EDFS’s command, and x-result.html to process POST requests for results gathered for CAT, GetPartitionLocations, and ReadPartition. For the ls command, we used x-ls-post.html to handle the form. We render all other commands’ results with x-ls.html. POST data are sent via key-value pairs and rendered accordingly regarding their purposes. 

## Analyze

#### The annual transmission output of the three businesses and the overall number of used vehicles:

<img src="./Final%20Report.assets/13391669697059_.pic-9697115.jpg" alt="13391669697059_.pic" style="zoom: 67%;" />

<img src="./Final%20Report.assets/13411669697060_.pic-9697160.jpg" alt="13411669697060_.pic" style="zoom: 67%;" />

<img src="./Final%20Report.assets/13431669697061_.pic.jpg" alt="13431669697061_.pic" style="zoom: 67%;" />

According to the three graphs above, the production of Audi's automated and semi-automated vehicles climbed dramatically in 2019 while the production of manual vehicles decreased. Toyota's manual vehicle production has been expanding year after year, and their auto cars are also being produced in large numbers, but their manual transmission is always at the top. Ford produces about 90% of its vehicles with a manual gearbox. Only 10% of Ford vehicles are equipped with gearboxes other than the manual. Ford and Toyota have shown little interest in semi-autonomous vehicles. Audi's Semi-Auto production has increased year after year, and in  2019 it surpassed the Manual Transmission.  Based on the foregoing, we believe that Audi outperforms Ford and Toyota in auto and semi auto transmission vehicles. On the other hand, Toyota and Ford are competing in the manual gearbox market. 

#### The car's resale value differs from its  original retail price:

The market value has been calculated on this formula: Current market value = Release Price * (1 - depreciation rate) ^ (2022-release year).
The depreciation rate for all cars and companies has been assumed as 12.5% every year.
The 2022 minus release year is the current age of the car



According to the formula that was shown before, the link between pricing and current market value may be seen in the three graphs that have been presented above for Audi, Ford, and  Toyota. Based on the findings, we are able to draw the conclusion that there is a positively strong linear relationship between price and market value across all of the firms. This relationship indicates  that if a price is high, the pace at which its market value drops will be slower.

## **Learning Experience**





## **Conclusion**

After this project, we have deep learning about how to implement an emulation-based system for distributed file storage and parallel computation and how to deal with big data. We also learned more about the web development framework Django and gathered some hands-on experience with it. We now know how to set up a Django project with sub-projects, render pages with POST data (learned that one of the best ways is to use key-value pairs), how to redirect to another page, and how use Bootstrap with Django. Besides, we also learned how to do data analysis by using Python. 



## **Links**