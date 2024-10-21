import glob
import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import zipfile
import io
import boto3

URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"

response = requests.get(URL)
with open('feed.xml', 'wb') as file:
    file.write(response.content)

root = ET.fromstring(response.text)

count = 0

for i in root.findall('result'):
    for j in i.findall('doc'):
        file_type = j.find('.//str[@name="file_type"]').text
        if file_type == 'DLTINS':
            count += 1
            if count == 2:
                link = j.find('.//str[@name="download_link"]').text
                req = requests.get(link)
                with open('downloaded_file.zip', 'wb') as f:
                    f.write(req.content)
                break

unzip_folder = "unzipped_files"
with zipfile.ZipFile('downloaded_file.zip') as zip_file:
    zip_file.extractall(unzip_folder)

def xml_to_csv(xml_folder, csv_file):
    cols = ["FinInstrmGnlAttrbts.Id", 
            "FinInstrmGnlAttrbts.FullNm", 
            "FinInstrmGnlAttrbts.ClssfctnTp", 
            "FinInstrmGnlAttrbts.CmmdtyDerivInd", 
            "FinInstrmGnlAttrbts.NtnlCcy", 
            "Issr"]
    rows = []
    xml_files = glob.glob(os.path.join(xml_folder, '*.xml'))

    for xml_file in xml_files:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for instr in root.findall('.//FinInstrmGnlAttrbts'):
            id_ = instr.find('Id').text if instr.find('Id') is not None else ''
            full_nm = instr.find('FullNm').text if instr.find('FullNm') is not None else ''
            clssfctn_tp = instr.find('ClssfctnTp').text if instr.find('ClssfctnTp') is not None else ''
            cmmdty_deriv_ind = instr.find('CmmdtyDerivInd').text if instr.find('CmmdtyDerivInd') is not None else ''
            ntnl_ccy = instr.find('NtnlCcy').text if instr.find('NtnlCcy') is not None else ''
            issr = instr.find('Issr').text if instr.find('Issr') is not None else ''
            
            rows.append({
                "FinInstrmGnlAttrbts.Id": id_,
                "FinInstrmGnlAttrbts.FullNm": full_nm,
                "FinInstrmGnlAttrbts.ClssfctnTp": clssfctn_tp,
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": cmmdty_deriv_ind,
                "FinInstrmGnlAttrbts.NtnlCcy": ntnl_ccy,
                "Issr": issr
            })

    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(csv_file, index=False)

xml_to_csv('unzipped_files', 'csv_file.csv')
df = pd.read_csv('csv_file.csv')

df['a_count'] = df['FinInstrmGnlAttrbts.FullNm'].apply(lambda x: str(x).count('a') if pd.notnull(x) else 0)
df['contains_a'] = df['a_count'].apply(lambda x: 'YES' if x > 0 else 'NO')
df.to_csv('final_file.csv', index=False) 

bucket = 'bucket_name'
csv_buffer = io.StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'final_file.csv').put(Body=csv_buffer.getvalue())
