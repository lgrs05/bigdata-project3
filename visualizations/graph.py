import plotly.plotly as plty
from plotly.graph_objs import *
import datetime

fi = open('keywords.txt', 'r')

sentiment_scores = []
for record in fi:
    record = record[:-1]
    record = eval(record)
    sentiment_scores.append((record['timestamp'], record['positive'] / float((record['positive'] + record['negative']))))

fi.close()

trace_list = []
x_coordinates = []
y_coordinates = []
for point in sentiment_scores:
    x_coordinates.append(point[0])
    y_coordinates.append(point[1])

trace_list.append(Scatter(
    x=x_coordinates,
    y=y_coordinates,
    name="Positive Tweets Percentage"
))

output = Data(trace_list)

plty.plot(output, filename ='sentiment_analysis_graph')
