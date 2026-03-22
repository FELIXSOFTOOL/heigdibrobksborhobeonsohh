import requests 
 
TOKEN = "553676:AAx9zOTNwuDTKQbCnTjuGiM3Kpn4858VPZt" 
headers = {"Crypto-Pay-API-Token": TOKEN} 
 
data = { 
    "asset": "USDT", 
    "amount": "3.50" 
} 
 
r = requests.post( 
    "https://pay.crypt.bot/api/createInvoice", 
    headers=headers, 
    json=data 
) 
 
print(r.json())