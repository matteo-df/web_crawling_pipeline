import requests
import dns.resolver
import pycountry

def get_ip_for_domain(domain):
  try:
    result = dns.resolver.resolve(domain, 'A')
    return result[0].to_text()
  except Exception as e:
    print(f'Error resolving domain: {e}')
    return None


def get_country_for_ip(ip):
  try:
    response = requests.get(f'https://ipinfo.io/{ip}/json')
    data = response.json()
    country_code = data.get('country')
    country = pycountry.countries.get(alpha_2=country_code)
    return country.name if country else 'Unknown'
  except Exception as e:
    print(f'Error retrieving information: {e}')
    return None


def get_country_for_domain(domain):
  ip = get_ip_for_domain(domain)
  if ip:
    return get_country_for_ip(ip)
  return None