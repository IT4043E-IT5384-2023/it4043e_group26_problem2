import pickle

similarity = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\similarity.pkl','rb'))
df = pickle.load(open(r'F:\Mr.Hung\Team12-Problem3\deploy_model\df.pkl','rb'))


def recommendation(hash_tag):
    idx = df[df['hashtags'] == hash_tag].index[0]
    print(idx)
    distances = sorted(list(enumerate(similarity[idx])),reverse=True,key=lambda x:x[1])
    
    recommends = []
    for m_id in distances[1:20]:
        if df.iloc[m_id[0]].full_text in recommends:
          continue
        recommends.append(df.iloc[m_id[0]].full_text)
        if len(recommends) == 4:
          break
        
    return recommends

recommendations = recommendation('Giveaway')
# print(recommendations.encode("utf-8"))
for rcm in recommendations:
   rcm_encode = rcm.encode("utf-8")
   print(rcm_encode)