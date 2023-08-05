# _Los Propios Vectores_ - Factored's Hackathon Summer 2023 🚀
This is the place to store all project-related files. This includes code, documentation, assets, and any other resources used during the hackathon. It helps keep things organized and easily accessible. 🧹🧹

Keeping the repository public allows other developers and enthusiasts to learn from the project. It can inspire others and contribute to the collective knowledge within the coding community.

## Who?
Colombian 🇨🇴 team with Paisa flavor and *loads* of financial DS/DE experience.
- [Carlos](https://carloseduardo.omg.lol/) @carlose-cortesg: Gray areas, between Data Engineering, finance, and anth. Likes watching birds 🦩, Airflow, and heteroskedasticity. 
- [Mateo](https://www.linkedin.com/in/mateo-graciano-data-scientist/) @magralo: The rabbit hole of Data Engineering + Applied Math. Likes competitive Pokemon 🧪, Bayes Theorem, and Deportivo Independiente Medellin. 
- [David](https://www.linkedin.com/in/david-baena-castro-800174168/) @david16baena: Math, Math and Math. Cake Enthusiast 🍰 (Really!)

## Repo Rules
Playground rules that allow happy coding: Keeping track of the code changes, collaborating seamlessly, and easily revert to previous versions if necessary.

- Staging and main branches are protected -> PRs and approvals are a must! before pushing changes someone needs to approve
- Commit windows that are every day between 19:30 EST and 21:30 EST
- Black formatting for Python is enforced!
- Squash and Merge! 🎇


## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

Be kind!

Play to learn rather than win!

## License

[MIT](https://choosealicense.com/licenses/mit/)


# How to deploy the streaming listener

1. Create a VM with linux
2. Intall anaconda
3. Install the dependencies:
    - google-cloud-bigquery==3.11.4
    - azure-eventhub==5.11.3
    - db-dtypes==1.1.1
    - spacy==3.6.0
    - pandas==2.0.3
    - fast-sentence-transformers==0.4.1
4. Run the python script in deatached mode: nohup python streaming_data.py & 
5. If you ever need to re run the script
    - Identify the script id: pgrep -af python
    - kill the process: kill -SIGKILL XXXX
    - Re run (4)



# How to use API to ask any question

To run the api make a GET request to http://34.125.85.105:8000/help passing the parameters
'question' (question asked to reviews), 'product' (product id), 'k' (number of reviews show, that answers the question, **optional**), 'limit' (limits query on data to make faster inferences, **optional**)

```python
import requests
params = {
    'question':'does it has a good queality/price relation?',
    'product':'B0073WAK8K', 
    'k':5,              #optional
    'limit':'LIMIT 10'  #optional
         }

result = requests.get('http://34.125.85.105:8000/help', params)
```

```python
import requests
params = {
    'question':'does it has a good queality/price relation?',
    'product':'B0073WAK8K'
         }

result = requests.get('http://34.125.85.105:8000/help', params)
```


# Please check our Batch POC

[Looker Studio POC](https://lookerstudio.google.com/reporting/8cd42793-78f9-4403-a55e-edbe51c9e897/page/p_tvpz4o0i8c)