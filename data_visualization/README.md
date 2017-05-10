## Data visualization

This folder contains a mixture of Excel workbooks, Tableau workbooks, and Jupyter notebooks that we used to create our data visualizations.

#### data folder
This subfolder contains the csv files used to create the visualizations, which were created by taking the outputs of our Spark scripts. They are required to run the commands within the Jupyter notebook.

#### ipynb instructions
Download the data from the data folder and run all of the commands in Jupyter notebooks.

*Note: Photoshop was used to overlay the Rain and Snow plots onto the daily crime calendar plot*

#### Excel instructions
`borough_pop.xls`
See section 9.2 in the project report for the data sources and calculation methodology. A line plot was created using a Primary axis for the **crimes per capita** and a Secondary axis for **population density**.

`sentence_length.xls`
See section 9.3 in the project report for the data sources and calculation methodology. A line plot was created using a Primary axis for the **crime counts** and a Secondary axis for **maximum sentence length**.

`Date, DUI, Noise, and Race viz.xls`
This can be found on dropbox: https://www.dropbox.com/sh/xrxl0s2cck3gu8f/AADmT9yxogCy4ieRBYkEOrbSa?dl=0. Instructions for each of the visualizations can be found within separate tabs of the Excel file.

#### Tableau instructions
`borough_types.twb`
See section 8.11 in the project report for the calculation methodology. A bar plot was created using the columns of **SUM(Counts)** and rows of **Offense type**.  On **SUM(Counts)**, a Table Calculation of Percent of Total computed using Table (across) was selected.

`delay.twb`
See section 8.8 in the project report for the calculation methodology. A bar plot was created using the columns of **SUM(Average delay in days)** and rows of **Offense**. 

`shoplifting.twb`
See section 8.7 in the project report for the calculation methodology. A bar plot was created using the columns of **SUM(Counts)** and rows of **Store type**. On **SUM(Counts)**, a Table Calculation of Percent of Total computed using Table (across) was selected. Then, the Mark label was shown for the PETIT LARCENY bars. 

`valid_counts.twb`
See section 5 in the project report for the calculation methodology. A bar chart was created with columns of **SUM(Valid Count), SUM(Invalid Count),** and **SUM(NULL Count)**, with rows of **Col. Name**.

`weekday_pie.twb`
See section 8.4 in the project report for the calculation methodology. A pie chart was created with slices of **Day of week** for the **SUM(Total Counts of Crime)**. A Table Calculation of of Percent of Total computed using Table (across) was selected. The labeled white text was manually added to the plot.
