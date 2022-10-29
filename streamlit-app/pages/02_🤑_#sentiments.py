# Contents of ~/my_app/pages/page_2.py
import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import timedelta,datetime
from Historic_Crypto import HistoricalData


st.set_page_config(
    page_title="razanlytics",
    page_icon="🤑",
    layout="wide",
    initial_sidebar_state="expanded"
)


st.markdown("# Razanlytics")

st.header("Sentiments and BTCUSD Trends")

page = st.sidebar.selectbox('Select Trend Range',
  ['Daily','Monthly','Yearly'])


c1, c2 = st.columns(2)

date = st.slider(
     "Adjust Record Length:",datetime(2021, 12, 1),datetime(2022, 6, 1),datetime(2022, 6, 7))

t1 = str(date).replace(':','-').replace(' ','-')[:-3]
t2 = str(date+timedelta(days=1)).replace(':','-').replace(' ','-')[:-3]


df = HistoricalData('BTC-USD',3600,t1,t2).retrieve_data()


data = [go.Candlestick(x=df.index,open=df.open,high=df.high,low=df.low,close=df.close)]
layout = go.Layout(title='Bitcoin Candlestick with Range Slider',xaxis={'rangeslider':{'visible':True}})

fig = go.Figure(data=data,layout=layout)


c1.plotly_chart(fig,use_container_width = True)



df = pd.read_csv(f'./sentiments/sent_{str(date).split()[0]}.csv')

df['weighted_comp'] = df.compound*((df.likes -df.likes.mean())/df.likes.std())
df['timestamp'] = pd.to_datetime(df.timestamp)
df = df.resample('H', on='timestamp').mean()

fig = px.line(df, x=df.index, y=['compound', 'neg', 'pos','weighted_comp'],
              title='Sentiments')

c2.plotly_chart(fig,use_container_width = True)
