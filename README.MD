# Project overview

- In this project, I build models that predict energy prices for a specific node in the California Real-Time energy market. 
- Data extraction and cleaning files collect, transform and combine over 3 years of data from the California Independent System Operator's (CAISO) energy data API and Open-Meteo's weather data API, dating from April 2020 to July 2023. I then perform exploratory data analysis (EDA) on grid/energy, calendar, and weather data.
- I compare the results from Random Forest, XGBOOST, Deep Neural Network (DNN) and Recurrent Neural Network (RNN) models. The best model (Random Forest Regressor) achieves a Mean Absolute Error that is roughly 25% better than baseline 'persistence' model.

# Background

- Energy prices in the real time market are notoriously variable and prone to extreme values, making price prediction challenging. In my data, I observe that the maximum energy price is more than 40 times as high as the median price. Price prediction nonetheless remains important to market participants such as energy suppliers, traders, and consumers by allowing them to plan their energy production and consumption decisions.

# Data Collection
- For my target variable, I choose the simple hourly average of 5-minute real-time energy prices (Locational Marginal Price in $/MWh) at specific node in the San Diego Metropolitan Area (PACFCBCH_6_N004). 
- Features used for prediction include the following. 'Data_Extraction' and 'DataCleaning' files also include feature engineering based on these feature and calendar features.

|Feature      |Description|
|-------------|-----------|
|Day Ahead Energy Prices|Day Ahead hourly Locational Marginal Price and plus the Congestion, Loss and Energy Components that make up the LMP|
|Wind and Solar Forecast|Forecast and actual wind and solar generation by hour|
|System Load and Resource Schedules|System Load, Generation, Import and Export|
|Temperature| Temperature for the appropriate latitude / longitude was downloaded from the Open-Meteo historical weather API. For more information see Open-Meteo Documentation (https://open-meteo.com/en/docs/historical-weather-api)|

## Terminology
The following terms and acronyms are used throughout the notebook
- **Day-Ahead (DA) v. Real-Time (RT) markets:** The Day-Ahead market operates a day before the actual delivery of electricity and allows market participants to schedule resources in advance based on their forecasts of electricity demand and generation availability for the next day. The Real-Time market operates on a 5-minute bases and is designed to address immediate changes in electricity demand and supply. The Real-Time market enables adjustments to match actual conditions as closely as possible.

- **Locational Marginal Price (LMP)**:  LMP is a locational-based pricing system that reflects the marginal cost of supplying or consuming electricity at specific locations on the power grid. This represents the target variable that my models seek to predict.

# EDA
In my EDA file I analyze the distribution of real-time energy prices and their relationships to the feature variables. Below are a few plots that emphasize the skewed distribution of LMP and its relationship to the feature set. See the EDA notebook for more detail. Below are some interesting results from the initial exploration. 

- **Both Real-Time and Day-Ahead prices are subject to extremely high outliers well above the mean.**
![LMP_Dist](https://github.com/Lbongard/energy_project/assets/62773555/53ec4eab-a788-4f33-a8e9-3bb72a0ef5c9)

- **Among grid and price-related variables, Day-Ahead LMP and MW of Generation are most correlated with Real Time LMP**
![corr_heatmap](https://github.com/Lbongard/energy_project/assets/62773555/fb357ae6-e639-4659-b834-7585b86fbe6c)

- **Average Real-Time LMP experiences a ‘duck-like’ shape throughout the day, similar to the infamous CAISO net load “duck curve”.** The similarity of the two curves are likely related to the same phenomenon — namely, the high supply of solar energy during the day and the large demand “ramp” in the evening hours when solar generation is subsiding.

![avg_RTLMP_by_hour](https://github.com/Lbongard/energy_project/assets/62773555/837c8b94-a281-4317-849d-c9b9c1267aaf)

Source: https://www.energy.gov/eere/articles/confronting-duck-curve-how-address-over-generation-solar-energy
![CAISO_DuckCurve_720_469_80](https://github.com/Lbongard/energy_project/assets/62773555/9c732be1-0dcd-41cf-91d7-4e8d637bdf73)


# Energy Prediction Models

For my price prediction models, I predicted the simple average of the Real Time LMP 2 hours “in the future” (i.e., from the timestamp of the feature variables). I compare the results from 6 different models:

- Persistence model: Used as a baseline model for comparison. Predicts the target LMP to be the same as the current price or the price 24 hours prior to the target time
- Random Forest Regressor
- XGBoost Regressor
- Deep Neural Network (DNN)
- Two Flavors of Recurrent Neural Networks (RNNs): Long Short-Term Memory (LSTM) and Gated Recurrent Unit (GRU) models. Unlike the other models, these take a 24-hour “lookback” period of the feature variables as an input. In my GRU model, I implement regularization and increase the number of epochs.

Of all models, the Random Forest achieves the best performance for all metrics except Mean Absolute Percent Error. Interestingly, the 'current hour' persistence model has the lowest Mean Absolute Percent Error. Comparing observations where the Random Forest percentage error is much higher than the Persistence model percentage error, we can see that this phenomenon is likely driven by instances where the target value is close to 0. In other words, slight increases in over- or under-prediction will lead to large increases in MAPE.

| Model             |   r2 |   rmse |   mae |   mape |
|:------------------|-----:|-------:|------:|-------:|
| persist_prev_day  | 0.41 |  68.19 | 27.75 |  24.78 |
| persist_curr_hour | 0.47 |  64.47 | 23.84 |   6.6  |
| Random Forest     | 0.69 |  49.36 | 20.49 |  25.09 |
| XGBoost           | 0.58 |  57.75 | 25.17 |  20.81 |
| DNN               | 0.3  |  74.37 | 43.21 |  18.58 |
| LSTM              | 0.44 |  67.03 | 32.56 |  30.59 |
| GRU               | 0.59 |  57.27 | 25.4  |  20.93 |

![RF_Pred](https://github.com/Lbongard/energy_project/assets/62773555/4806a456-edfd-4777-9616-65eac1006ae8)

Analyzing feature importances, we can see that lagged real-time and day-ahead market prices are the most important variables used by the model when making predictions. The current hour's Real-Time price is the most important variable in predicting the LMP two hours in the future, likely explaining the relatively high performance of the baseline Persistence model. Some grid-condition variables such as generation / export also appear to be important to the model.
![RF_Feature_Importances](https://github.com/Lbongard/energy_project/assets/62773555/14f71c1d-b64b-423e-8659-fe9640fa4dad)
