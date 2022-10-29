# Contents of ~/my_app/main_page.py
import streamlit as st
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
from urllib.request import urlopen
import json
import requests


st.set_page_config(
    page_title="razanlytics",
    page_icon="🤑",
    layout="wide",
    initial_sidebar_state="expanded"
)

c1, c2, c3 = st.columns(3)
with c2:
    st.markdown("# Razanlytics Dashboard")






class Tweet(object):
    def __init__(self, s, embed_str=False):
        if not embed_str:
            # Use Twitter's oEmbed API
            # https://dev.twitter.com/web/embedded-tweets
            api = "https://publish.twitter.com/oembed?url={}".format(s)
            response = requests.get(api)
            self.text = response.json()["html"]
        else:
            self.text = s

    def _repr_html_(self):
        return self.text

    def component(self):
        return components.html(self.text, height=600)



forecast_range = st.sidebar.selectbox('Select Range for Forecasting',
  ['12hrs','32hrs'])

if forecast_range == '12hrs':
    filename = 'output12.csv'
else:
    filename = 'output32.csv'

df_res = pd.read_csv(filename)
df_res.timestamp = pd.to_datetime(df_res['timestamp'])

fig = px.line(df_res, x="timestamp", y=['predicted','previous'],
              title='Transformers Multivariate with sentiment scores')

st.plotly_chart(fig,use_container_width = True)




# Load market data from Binance API
data_json = json.loads(urlopen("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT").read())
col_price = round(float(data_json['weightedAvgPrice']),2)
col_percent = f"{float(data_json['priceChangePercent'])}%"


df_res = df_res.sort_values(by = ['timestamp'])


val = ( df_res['predicted'].iloc[-1]-col_price)/col_price
c= st.columns(3)
with c[0]:
    
    st.header("Current Price")
    st.metric("BTCBUSD", col_price, col_percent)

    st.header("Predicted Price")
    st.metric("BTCBUSD", round(df_res['predicted'].iloc[-1],3),f"{round(float(val),3)}%")
with c[1]:
    st.markdown("#### Today's Hottest bitcoin-related Tweet")
    t = Tweet("https://twitter.com/BTC_Archive/status/1533760097524006914").component()    

with c[2]:
    st.header("Todays Sentiments")
    
    st.write('sentiment score in last 24 hours,')
    st.write('positive: ',0.11848)
    st.write('negative: ',0.05858)


st.sidebar.image(
    "https://bitcoin.org/img/icons/opengraph.png?1652976465",
    width=50,
)


