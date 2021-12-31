# Name - Seshagiri Reddy
# Date - 03-05-2021
# Title - Assignment 3: Lending Club, Estimation 




#1 Read the dataset into R

# Set working Directory
setwd("C:/Users/satya/OneDrive - Texas State University/ShareKnowledge/Courses/QMST5334-STMD/Assignments/2")
# creating a dataframe with read.table function to load data into R from csv file with comma delimiter
lc_loans_df <- read.table(file = "loans_full_schema.csv", header = TRUE, sep = ",")




#2 t test for mean interest of sample at 99% confidance

t.test(x=lc_loans_df$interest_rate,conf.level = 0.99)
# I can say that at 99% confidence level, the estimated value of population mean for interest rate is between 12.29868 and 12.55637




#3 Compare the 2nd with loan grade B

# Loan grade B is between 13.33 and 16.08
# Estimated mean is between 12.29868 and 12.55637
# Both are not matching which show B is not a very good option to choose




#4

t.test(x=lc_loans_df$interest_rate[lc_loans_df$state == 'TX'],conf.level = 0.99)
# I can say that at 99% confidence level, the estimated value of population mean for interest rate of Texas state is between 11.76285 and 12.67800

# from  3 Loan grade B is between 13.33 and 16.08
# from 2 Estimated mean interest is between 12.29868 and 12.55637
# Estimated mean interest of Texas is between 11.76285 and 12.67800 which is broader than overall mean range, we might have to do another check that for Texas mean either greater than or less than the overall mean 
# Again B is not a very good option to choose as it is not matching 




#5

t.test(x=lc_loans_df$annual_income[lc_loans_df$state == 'TX'],conf.level = 0.99)
# I can say that at 99% confidence level, the estimated value of population mean for annual income of Texas state is between 75880.69  and 85352.27
