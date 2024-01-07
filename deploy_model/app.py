import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle

app = Flask(__name__)
similarity = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\similarity.pkl','rb'))
df = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\df.pkl','rb'))

def recommendation(screen_name):
    # idx = df[df['screen_name'] == screen_name].index[0]
    # idx là vị trí cột screen_name là 0
    distances = sorted(list(enumerate(similarity[0])),reverse=True,key=lambda x:x[1])
    # distances = sorted(list(enumerate(similarity[idx])),reverse=True,key=lambda x:x[1])
    
    recommends = []
    for m_id in distances[1:20]:
        if df.iloc[m_id[0]].full_text in recommends:
          continue
        recommends.append(df.iloc[m_id[0]].full_text)
        if len(recommends) == 4:
          break
        
    return recommends

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    screen_name = str(request.form.values())


    prediction = recommendation(screen_name)
    prediction_1 = "Suggestion #1: {}".format(prediction[0].encode("utf-8"))
    prediction_2 = "Suggestion #2: {}".format(prediction[1].encode("utf-8"))
    prediction_3 = "Suggestion #3: {}".format(prediction[2].encode("utf-8"))
    prediction_4 = "Suggestion #4: {}".format(prediction[3].encode("utf-8"))
    # prediction_text = 'Suggestion #1: \n{}\nSuggestion #2: {}\nSuggestion #3: {}\nSuggestion #4: {}'.format(prediction_1, prediction_2, prediction_3, prediction_4)
    return render_template('index.html', prediction_text_1=prediction_1,prediction_text_2=prediction_2,prediction_text_3=prediction_3,prediction_text_4=prediction_4)
    # return render_template('index.html', prediction_text='Employee Salary should be $ {}'.format(output))

# @app.route('/predict_api',methods=['POST'])
# def predict_api():
#     '''
#     For direct API calls trought request
#     '''
#     data = request.get_json(force=True)
#     prediction = model.predict([np.array(list(data.values()))])

#     output = prediction[0]
#     return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)