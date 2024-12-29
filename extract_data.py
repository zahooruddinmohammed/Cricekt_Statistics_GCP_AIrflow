import requests
import csv


url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

querystring = {"formatType":"test"}

headers = {
	"x-rapidapi-key": "9c40e645ebmshcbf6739629a4d5fp19d4f4jsn4412fe7c57a0",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code ==  200:
    data = response.json().get('rank',[])  #take the rank data out
    csv_filename = 'icc_ranking.csv'

    if data:
        field_name= ['rank','name','country']  #taking only what is needed

        with open(csv_filename,'w',newline='',encoding='utf-8') as csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=field_name)
            #writer.writeheader()
            for entry in data:
                writer.writerow({field:entry.get(field)for field in field_name})

        print(f"Data fetched succesfully and written to '{csv_filename}'")

    else:
        print("No data available for the API")
    
else:
    print("Failed to fetch data:",response.status_code)