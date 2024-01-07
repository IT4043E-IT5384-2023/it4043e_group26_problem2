import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle

app = Flask(__name__)
similarity = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\similarity.pkl','rb'))
df = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\df.pkl','rb'))

def recommendation(screen_name):
    # idx = df[df['hashtags'] == screen_name].index[0]
    distances = sorted(list(enumerate(similarity[0])),reverse=True,key=lambda x:x[1])
    
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
    print(screen_name)
    print(type(screen_name))
    prediction = recommendation(screen_name)

    return render_template('index.html', prediction_text='haizz')
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