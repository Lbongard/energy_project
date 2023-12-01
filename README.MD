# Project overview

- In this project, I build models that predict energy prices for a specific node in the California Real-Time energy market. 
- Data extraction and cleaning files collect, transform and combine over 3 years of data from the California Independent System Operator's (CAISO) energy data API and NOAA's publicly-available weather API, dating from April 2020 to July 2023. I then perform exploratory data analysis (EDA) on grid/energy, calendar, and weather data.
- I compare the results from Random Forest, XGBOOST, Deep Neural Network (DNN) and Recurrent Neural Network (RNN) models. The best model (x) achieves a Mean Absolute Error that is x% better than baseline 'persistence' model.

# Background

- Energy prices in the real time market are notoriously variable and prone to extreme values, making price prediction challenging. In my data, I observe that the maximum energy price is x times higher than the median price. Price prediction nonetheless remains important to market participants such as energy suppliers, traders, and consumers by allowing them to plan their energy production and consumption decisions.

# Data Collection
- For my target variable, I choose the simple hourly average of 5-minute real-time energy prices (Locational Marginal Price in $/MWh) at specific node in the San Diego Metropolitan Area (PACFCBCH_6_N004). 
- Features used for prediction include the following. 'Data_Extraction' and 'DataCleaning' files also include feature engineering based on these feature and calendar features.

|Feature      |Description|
|-------------|-----------|
|Day Ahead Energy Prices|Day Ahead hourly Locational Marginal Price and plus the Congestion, Loss and Energy Components that make up the LMP|
|Wind and Solar Forecast|Forecast and actual wind and solar generation by hour|
|System Load and Resource Schedules|System Load, Generation, Import and Export|
|Temperature| Temperature for the appropriate latitude / longitude was downloaded from the Open-Meteo historical weather API. For more information see Open-Meteo Documentation (https://open-meteo.com/en/docs/historical-weather-api)|

# EDA
In my EDA file I analyze the distribution of real-time energy prices and their relationships to the feature variables. Below are a few plots that emphasize the skewed distribution of LMP and its relationship to the feature set. See the EDA notebook for more detail. Below are some interesting results from the initial exploration. 

- Both Real-Time and Day-Ahead prices are subject to extremely high outliers well above the mean.
![alt text] (https://github.com/Lbongard/energy_project/blob/main/LMP_Dist.png?raw=true)

- Among grid and price-related variables, Day-Ahead LMP and MW of Generation are most correlated with Real Time LMP

- Average Real-Time LMP experiences a ‘duck-like’ shape throughout the day, similar to the infamous CAISO net load “duck curve”

# Energy Prediction Models



Of all models, the Random Forest achieves the best performance for all metrics except Mean Absolute Percent Error. Interestingly, the 'current hour' persistence model has the lowest Mean Absolute Percent Error. Comparing observations where the Random Forest percentage error is much higher than the Persistence model percentage error, we can see that this phenomenon is likely driven by instances where the target value is close to 0. In other words, slight increases in over- or underprediction will lead to large increases in MAPE.