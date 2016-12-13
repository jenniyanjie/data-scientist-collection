# reproduced from https://www.r-bloggers.com/outlier-detection-and-treatment-with-r/
# Author: Selva Prabhakaran
######### Why outlier treatment is important ######### 
# Inject outliers into data.
cars1 <- cars[1:30, ]  # original data
cars_outliers <- data.frame(speed=c(19,19,20,20,20), dist=c(190, 186, 210, 220, 218))  # introduce outliers.
cars2 <- rbind(cars1, cars_outliers)  # data with outliers.

# Plot of data with outliers.
par(mfrow=c(1, 2))
plot(cars2$speed, cars2$dist, xlim=c(0, 28), ylim=c(0, 230), 
     main="With Outliers", xlab="speed", ylab="dist", pch="*", col="red", cex=2)
abline(lm(dist ~ speed, data=cars2), col="blue", lwd=3, lty=2)

# Plot of original data without outliers. Note the change in slope (angle) of best fit line.
plot(cars1$speed, cars1$dist, xlim=c(0, 28), ylim=c(0, 230), 
     main="Outliers removed \n A much better fit!", 
     xlab="speed", ylab="dist", pch="*", col="red", cex=2)
abline(lm(dist ~ speed, data=cars1), col="blue", lwd=3, lty=2)


################### detect outliers ################# 
# --------------- univariate approach ---------------
# For a given continuous variable, outliers are those observations that lie outside 
# 1.5 * IQR, where IQR, the ‘Inter Quartile Range’ is the difference between 75th 
# and 25th quartiles. Look at the points outside the whiskers in below box plot.
url <- "http://rstatistics.net/wp-content/uploads/2015/09/ozone.csv"
ozone <- read.csv(url)

outlier_values <- boxplot.stats(ozone$pressure_height)$out  # outlier values.
boxplot(ozone$pressure_height, main="Pressure Height", boxwex=0.1)
mtext(paste("Outliers: ", paste(outlier_values, collapse=", ")), cex=0.6)

# --------------- Bivariate approach ----------------
# Visualize in box-plot of the X and Y, for categorical X’s
# For categorical variable
par(mfrow=c(1, 2))
boxplot(ozone_reading ~ Month, data=ozone, main="Ozone reading across months")  
# clear pattern is noticeable.
boxplot(ozone_reading ~ Day_of_week, data=ozone, main="Ozone reading for days of week")  
# this may not be significant, as day of week variable is a subset of the month var.

# For continuous variable (convert to categorical if needed.)
par(mfrow=c(1, 2))
boxplot(ozone_reading ~ pressure_height, data=ozone, 
        main="Boxplot for Pressure height (continuos var) vs Ozone")
boxplot(ozone_reading ~ cut(pressure_height, pretty(ozone$pressure_height)), 
        data=ozone, main="Boxplot for Pressure height (categorial) vs Ozone", cex.axis=0.5)

# ---------- Multivariate Model Approach -------------
# Cook’s Distance
# Cook’s distance is a measure computed with respect to a given regression model 
# and therefore is impacted only by the X variables included in the model. 
# But, what does cook’s distance mean? It computes the influence exerted by each 
# data point (row) on the predicted outcome.
# The cook’s distance for each observation i measures the change in Ŷ Y^ (fitted Y)
# for all observations with and without the presence of observation i, so we know how 
# much the observation i impacted the fitted values.

mod <- lm(ozone_reading ~ ., data=ozone)
cooksd <- cooks.distance(mod)

# Influence measures
# In general use, those observations that have a cook’s distance greater than 4 
# times the mean may be classified as influential. This is not a hard boundary.

plot(cooksd, pch="*", cex=2, main="Influential Obs by Cooks distance")  # plot cook's distance
abline(h = 4*mean(cooksd, na.rm=T), col="red")  # add cutoff line
text(x=1:length(cooksd)+1, y=cooksd, labels=ifelse(cooksd>4*mean(cooksd, na.rm=T),names(cooksd),""), 
     col="red")  # add labels

# Now lets find out the influential rows from the original data. If you extract and examine each 
# influential row 1-by-1 (from below output), you will be able to reason out why that row turned 
# out influential. It is likely that one of the X variables included in the model had extreme values.

influential <- as.integer(names(cooksd)[cooksd >  4*mean(cooksd, na.rm=T)])  # influential row numbers
head(ozone[influential, ])  # influential observations.

# ################## Outliers package ################# 
# The outliers package provides a number of useful functions to systematically extract outliers. 
# Some of these are convenient and come handy, especially the outlier() and scores() functions.

# -------------------- Outliers -----------------------
# outliers gets the extreme most observation from the mean. If you set the argument opposite=TRUE, 
# it fetches from the other side.
library(outliers)

set.seed(1234)
y=rnorm(100)
outlier(y)
#> [1] 2.548991
outlier(y,opposite=TRUE)
#> [1] -2.345698
dim(y) <- c(20,5)  # convert it to a matrix
outlier(y)
#> [1] 2.415835 1.102298 1.647817 2.548991 2.121117
outlier(y,opposite=TRUE)
#> [1] -2.345698 -2.180040 -1.806031 -1.390701 -1.372302

# -------------------- Scores -----------------------
# There are two aspects the the scores() function.
# Compute the normalised scores based on “z”, “t”, “chisq” etc
# Find out observations that lie beyond a given percentile based on a given score.

set.seed(1234)
x = rnorm(10)
scores(x)  # z-scores => (x-mean)/sd
scores(x, type="chisq")  # chi-sq scores => (x - mean(x))^2/var(x)
#> [1] 0.68458034 0.44007451 2.17210689 3.88421971 0.66539631  . . .
scores(x, type="t")  # t scores
scores(x, type="chisq", prob=0.9)  # beyond 90th %ile based on chi-sq
#> [1] FALSE FALSE FALSE  TRUE FALSE FALSE FALSE FALSE FALSE FALSE
scores(x, type="chisq", prob=0.95)  # beyond 95th %ile
scores(x, type="z", prob=0.95)  # beyond 95th %ile based on z-scores
scores(x, type="t", prob=0.95)  # beyond 95th %ile based on t-scores

# ################## Treatment ################# 
# Once the outliers are identified, you may rectify it by using one of the following approaches.

# -------------------- Imputation -----------------------
# Imputation with mean / median / mode. This method has been dealt with in detail in the discussion 
# about treating missing values. Another robust method which we covered at DataScience+ is 
# multivariate imputation by chained equations.
# 
# -------------------- Capping -----------------------
# For missing values that lie outside the 1.5 * IQR limits, we could cap it by replacing those 
# observations outside the lower limit with the value of 5th %ile and those that lie above the 
# upper limit, with the value of 95th %ile. Below is a sample code that achieves this.

x <- ozone$pressure_height
qnt <- quantile(x, probs=c(.25, .75), na.rm = T) 
caps <- quantile(x, probs=c(.05, .95), na.rm = T)
H <- 1.5 * IQR(x, na.rm = T)
x[x < (qnt[1] - H)] <- caps[1]

x[x > (qnt[2] + H)] <- caps[2]

# -------------------- Prediction -----------------------

# In yet another approach, the outliers can be replaced with missing values NA and then can 
# be predicted by considering them as a response variable. We already discussed how to predict 
# missing values.
