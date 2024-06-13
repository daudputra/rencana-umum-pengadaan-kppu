import scrapy
import requests
import sys
sys.path.append('../../../')
from tools.saveto.save_json import save_json
import os
import time
from datetime import datetime

class RupkSpider(scrapy.Spider):
    name = "rupk_swakelola"
    allowed_domains = ["sirup.lkpp.go.id"]
    start_urls = ['https://sirup.lkpp.go.id/sirup/datatablectr/datarupswakelolakldi?idKldi=L32&tahun=2024&sEcho=2&iColumns=8&sColumns=%2C%2Cnama%2C%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=false&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&mDataProp_7=7&sSearch_7=&bRegex_7=false&bSearchable_7=true&bSortable_7=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&sRangeSeparator=~']

    # cookies = {
    #     'PLAY_SESSION': 'de63d5496a265bdf0ee5d77d26bf0967e9f75837-___TS=1717588997330&tahunAnggaranPilihan=2024&___ID=df37a8a0-0e29-4c81-9108-31fa27e7dd1c',
    #     '_cfuvid': 'zk0n.xxYvrP8TSHYxnkKBsRvN8cpZpS4pKbbWFHW0GE-1717573649652-0.0.1.1-604800000',
    #     '_ga': 'GA1.1.1575704632.1717573648',
    #     'cf_clearance': 'IVyausDXTC2.WbEpPboNo8zhO4X4zA7xVU6HDhIrEZE-1717586832-1.0.1.1-xMDo6Vc.Ns594EPEsxcBFx1gDVnlinHO5HB.SlkE78hhmW4atq6gYlwRyWps_jPCouxXy.zBslB8DyAWG28pYQ',
    #     '_ga_D78WKTMJC6': 'GS1.1.1717586831.2.1.1717587191.0.0.0',
    # }

    # headers = {
    #     'accept': 'application/json, text/javascript, */*; q=0.01',
    #     'accept-language': 'en-US,en;q=0.9,id;q=0.8',
    #     'cookie': 'PLAY_SESSION=de63d5496a265bdf0ee5d77d26bf0967e9f75837-___TS=1717588997330&tahunAnggaranPilihan=2024&___ID=df37a8a0-0e29-4c81-9108-31fa27e7dd1c; _cfuvid=zk0n.xxYvrP8TSHYxnkKBsRvN8cpZpS4pKbbWFHW0GE-1717573649652-0.0.1.1-604800000; _ga=GA1.1.1575704632.1717573648; cf_clearance=IVyausDXTC2.WbEpPboNo8zhO4X4zA7xVU6HDhIrEZE-1717586832-1.0.1.1-xMDo6Vc.Ns594EPEsxcBFx1gDVnlinHO5HB.SlkE78hhmW4atq6gYlwRyWps_jPCouxXy.zBslB8DyAWG28pYQ; _ga_D78WKTMJC6=GS1.1.1717586831.2.1.1717587191.0.0.0',
    #     'priority': 'u=1, i',
    #     'referer': 'https://sirup.lkpp.go.id/sirup/rekap/swakelola/L32',
    #     'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    #     'sec-ch-ua-mobile': '?0',
    #     'sec-ch-ua-platform': '"Windows"',
    #     'sec-fetch-dest': 'empty',
    #     'sec-fetch-mode': 'cors',
    #     'sec-fetch-site': 'same-origin',
    #     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    #     'x-requested-with': 'XMLHttpRequest',
    # }

    # params = {
    #     'idKldi': 'L32',
    #     'tahun': '2024',
    #     'sEcho': '2',
    #     'iColumns': '8',
    #     'sColumns': ',,nama,,,,,',
    #     'iDisplayStart': '0',
    #     'iDisplayLength': '100',
    #     'mDataProp_0': '0',
    #     'sSearch_0': '',
    #     'bRegex_0': 'false',
    #     'bSearchable_0': 'true',
    #     'bSortable_0': 'false',
    #     'mDataProp_1': '1',
    #     'sSearch_1': '',
    #     'bRegex_1': 'false',
    #     'bSearchable_1': 'true',
    #     'bSortable_1': 'true',
    #     'mDataProp_2': '2',
    #     'sSearch_2': '',
    #     'bRegex_2': 'false',
    #     'bSearchable_2': 'true',
    #     'bSortable_2': 'true',
    #     'mDataProp_3': '3',
    #     'sSearch_3': '',
    #     'bRegex_3': 'false',
    #     'bSearchable_3': 'true',
    #     'bSortable_3': 'true',
    #     'mDataProp_4': '4',
    #     'sSearch_4': '',
    #     'bRegex_4': 'false',
    #     'bSearchable_4': 'true',
    #     'bSortable_4': 'true',
    #     'mDataProp_5': '5',
    #     'sSearch_5': '',
    #     'bRegex_5': 'false',
    #     'bSearchable_5': 'true',
    #     'bSortable_5': 'true',
    #     'mDataProp_6': '6',
    #     'sSearch_6': '',
    #     'bRegex_6': 'false',
    #     'bSearchable_6': 'true',
    #     'bSortable_6': 'true',
    #     'mDataProp_7': '7',
    #     'sSearch_7': '',
    #     'bRegex_7': 'false',
    #     'bSearchable_7': 'true',
    #     'bSortable_7': 'true',
    #     'sSearch': '',
    #     'bRegex': 'false',
    #     'iSortCol_0': '0',
    #     'sSortDir_0': 'asc',
    #     'iSortingCols': '1',
    #     'sRangeSeparator': '~',
    # }

    # response = requests.get(
    #     'https://sirup.lkpp.go.id/sirup/datatablectr/datarupswakelolakldi',
    #     params=params,
    #     cookies=cookies,
    #     headers=headers,
    # )

    def start_requests(self):
        # url = "https://sirup.lkpp.go.id/sirup/datatablectr/dataruppenyediakldi"
        # yield scrapy.FormRequest(
        #     url,
        #     formdata=self.params,
        #     cookies=self.cookies,
        #     headers=self.headers,
        #     callback=self.parse
        # )
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)
        

    def parse(self, response):
        data = response.json()
        if 'aaData' in data:    
            for item in data['aaData']:
                id = item[0]
                satuan_kerja = item[1]
                nama_paket = item[2]
                pagu_rp = item[3]
                sumber_dana_api = item[4]
                kode_rup = item[5]
                waktu = item[6]
                kegiatan = item[7]

                detail_url = f'https://sirup.lkpp.go.id/sirup/home/detailPaketSwakelolaPublic2017/{id}'
                if detail_url:
                    yield scrapy.Request(detail_url, callback=self.detail_page, meta={
                        'id' : id,
                        'satuan_kerja' : satuan_kerja,
                        'nama_paket' : nama_paket,
                        'pagu_rp' : pagu_rp,
                        'sumber_dana_api' : sumber_dana_api,
                        'kode_rup' : kode_rup,
                        'waktu' : waktu,
                        'kegiatan' : kegiatan
                    })

    def detail_page(self, response):
        id = response.meta.get('id')
        satuan_kerja = response.meta.get('satuan_kerja')
        nama_paket = response.meta.get('nama_paket')
        pagu_rp = response.meta.get('pagu_rp')
        sumber_dana_api = response.meta.get('sumber_dana_api')
        kode_rup = response.meta.get('kode_rup')
        waktu = response.meta.get('waktu')
        kegiatan = response.meta.get('kegiatan')





        detail_data = {}
        
        table_rows = response.css('tr')
        for row in table_rows:
            label = row.css('td.label-left::text').get()
            if label is not None:
                label = label.strip()
            else:
                label = None
            value = row.css('td:not(.label-left)::text, td > span::text').get()
            if value is not None:
                value = value.strip()
            else:
                value = None
            detail_data[label] = value
        
        lokasi_pekerjaan_table = response.css('td.label-left:contains("Lokasi Pekerjaan") + td > table > tr')
        lokasi_pekerjaan = []
        for row in lokasi_pekerjaan_table:
            no = row.css('td:nth-child(1)::text, td > span::text').get()
            provinsi = row.css('td:nth-child(2)::text, td > span::text').get()
            kabupaten_kota = row.css('td:nth-child(3)::text, td > span::text').get()
            detail_lokasi = row.css('td:nth-child(4)::text, td > span::text').get()
           
            if all([no, provinsi, kabupaten_kota, detail_lokasi]):
                lokasi_pekerjaan.append({
                    'No': no,
                    'Provinsi': provinsi,
                    'Kabupaten/Kota': kabupaten_kota,
                    'Detail Lokasi': detail_lokasi,
                })
        detail_data['Lokasi Pekerjaan'] = lokasi_pekerjaan

        
        sumber_dana_table = response.css('td.label-left:contains("Sumber Dana") + td table tr')
        sumber_dana = []
        for row in sumber_dana_table:
            no = row.css('td:nth-child(1)::text, td > span::text').get()
            sumber_dana_value = row.css('td:nth-child(2)::text, td > span::text').get()
            ta = row.css('td:nth-child(3)::text, td > span::text').get()
            klpd = row.css('td:nth-child(4)::text, td > span::text').get()
            mak = row.css('td:nth-child(5)::text, td > span::text').get()
            pagu = row.css('td:nth-child(6)::text, td > span::text').get()
            tot_pagu = row.css('th:nth-child(2)::text ,td > span::text').get()

            if all([no, sumber_dana_value, ta, klpd, mak, pagu]):
                sumber_dana.append({
                    'No': no,
                    'Sumber Dana': sumber_dana_value,
                    'T.A.': ta,
                    'KLPD': klpd,
                    'MAK': mak,
                    'Pagu': pagu,
                    'Total Pagu' : tot_pagu
                })

        detail_data['Sumber Dana'] = sumber_dana
        

        jadwal_pelaksanaan_kontrak_table = response.css('td.label-left:contains("Jadwal Pelaksanaan Kontrak") + td table tr')
        jadwal_pelaksanaan_kontrak = []
        for row in jadwal_pelaksanaan_kontrak_table:
            mulai = row.css('td.mid:nth-child(1)::text').get()
            akhir = row.css('td.mid:nth-child(2)::text').get()
            if all([mulai, akhir]):
                jadwal_pelaksanaan_kontrak.append({
                    'Mulai': mulai,
                    'Akhir': akhir
                })

        detail_data['Jadwal Pelaksanaan Kontrak'] = jadwal_pelaksanaan_kontrak



        dir_raw = 'data'
        os.makedirs(dir_raw ,exist_ok=True)
        filename = f'{id}.json'
        local_path = f'D:/Visual Studio Code/Project Done/Magang Github/API/rencana_umum_pengadaan_kppu/rencana_umum_pengadaan_kppu/data/{filename}'
        path_s3 = f's3://ai-pipeline-raw-data/data/data_descriptive/kppu/e-procurement/rencana_umum_pengadaan_kppu/swakelola/json/{filename}'
        data = {
            'link' : 'https://sirup.lkpp.go.id/sirup/rekap/penyedia/L32',
            'link_detail' : response.url,
            'domain' : 'sirup.lkpp.go.id',
            'crawling_time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'crawling_time_epoch' : int(datetime.now().timestamp()),
            'path_data_raw' : path_s3,
            'path_data_clean' : None,
            'id' : id,
            'satuan_kerja' : satuan_kerja,
            'nama_paket' : nama_paket,
            'pagu_rp' : pagu_rp,
            'sumber_dana' : sumber_dana_api,
            'kode_rup' : kode_rup,
            'waktu' : waktu,
            'kegiatan' : kegiatan,
            'detail_data' : detail_data
        }
        save_json(data, os.path.join(dir_raw, filename))
        # upload_to_s3(local_path, path_s3.replace('s3://', ''))

