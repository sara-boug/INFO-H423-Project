from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error


class DataForcaster:
    p = 0
    q = 0
    d = 1
    delays = None

    def __init__(self, dframe_data: pd.DataFrame):
        self.dframe_data = dframe_data

    def set_data_shape(self):
        self.dframe_data.index = pd.to_datetime(self.dframe_data.index, format='%H:%M:%S', infer_datetime_format=True)
        self.dframe_data = self.dframe_data.groupby(pd.Grouper(freq='30s')).mean()
        self.dframe_data['time'] = pd.DatetimeIndex(self.dframe_data.index).values
        # we need to set 00 values to 24
        self.dframe_data.loc[
            self.dframe_data['time'] < datetime(year=1900, month=1, day=1, hour=4, minute=00),
            'time'] = self.dframe_data['time'] + timedelta(days=1)
        self.dframe_data = self.dframe_data.sort_values(by=['time'])
        self.dframe_data['time'] = self.dframe_data['time'].dt.time
        self.dframe_data.reset_index(drop=True, inplace=True)
        self.dframe_data.dropna(subset=['delay'], inplace=True)
        self.dframe_data = self.dframe_data.set_index(['time'])
        self.delays = self.dframe_data['delay'].tolist()

    def plot(self, delay=None, d=0):
        plt.minorticks_off()
        if delay is None:
            delay = self.delays
        time = self.dframe_data.index.astype(str).tolist()
        for i in range(d): time.pop()
        fig, axes = plt.subplots()
        axes.plot(time, delay, linewidth=1, color='orange')
        axes.set_xlabel('time of the day')
        axes.set_ylabel('delay (s)')
        axes.set_xticks(time[::150], minor=False)
        axes.grid()
        plt.show()

    def autocorrelation(self, data=None):
        if data is None:
            data = self.delays
        plot_acf(x=data, lags=25)
        plt.show()

    def partial_autocorrelation(self, data=None):
        if data is None:
            data = self.delays
        plot_pacf(x=data, lags=25)
        plt.show()

    def define_d_param(self, plot=None):
        """
        For d = 1 or d = 2  the ACF shows the same behavior,
        hence not that much reliable to estimate the d,
        hence, we rely on the d leading to the lowest
         standard deviation
        """
        self.d = 1
        model = ARIMA(self.delays, order=(0, self.d, 0))
        model_fit = model.fit(disp=0)
        data = model_fit.resid
        print(model_fit.summary())

    """ order of autoregressive """

    def define_p_q_param(self, plot=None):
        """
        Rule 6: If the PACF of the differenced series displays
         a sharp cutoff and/or the lag-1 autocorrelation is
          positive--i.e., if the series appears slightly
          "underdifferenced"--then consider adding an AR term to the model. The lag at which
        the PACF cuts off is the indicated number of AR terms

        Rule 7: If the ACF of the differenced series displays a sharp cutoff
        and/or the lag-1 autocorrelation is negative--i.e., if the series appears
         slightly "overdifferenced"--then consider adding an MA term to the model.
        The lag at which the ACF cuts off is the indicated number of MA terms.
        """
        self.p = 0
        self.q = 0

        model = ARIMA(self.delays, order=(0, 0, 0))
        model_fit = model.fit(disp=1)
        data = model_fit.resid
        self.autocorrelation(data)
        self.partial_autocorrelation(data)

    def perform_training(self):
        data_size = len(self.delays)
        percentage = int(data_size * 0.66)
        train, test = self.delays[0:percentage], self.delays[percentage:data_size - 1]
        history = [element for element in train]
        predictions = list()
        for i in range(len(test)):
            model = ARIMA(history, order=(self.p, self.d, self.q))
            model_fit = model.fit()
            predicted = (model_fit.forecast())
            predictions.append(predicted[0])
            expected = test[i]
            history.append(expected)
        mse = mean_squared_error(test, predictions)
        plt.plot(test)
        plt.plot(predictions, color='red', alpha=0.5)
        plt.title(' The mean Squared Error {}'.format(mse))
        plt.show()
