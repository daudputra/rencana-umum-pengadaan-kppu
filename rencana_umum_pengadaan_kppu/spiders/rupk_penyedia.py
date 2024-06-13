import sys
sys.path.append('../../../')
from tools.saveto.save_json import save_json
import scrapy
import requests
import os
from datetime import datetime

class RupkSpider(scrapy.Spider):
    name = "rupk"
    allowed_domains = ["sirup.lkpp.go.id"]
    cookies = {
        'PLAY_SESSION': '4f1dfbd690a108c980817e06b7206492613fc6f3-___TS=1717575615993&tahunAnggaranPilihan=2024&___ID=76359e5f-dfb3-4d28-b130-d8cad0a6b51d',
        '_cfuvid': 'NiCmlRQHkysyvpySU8EpuutYKSqMyluUhLyHya29uvg-1717121340377-0.0.1.1-604800000',
        '_ga': 'GA1.1.1063132374.1717121341',
        'cf_clearance': 'U0jRIguPPey.PljlozZbZ3AMzhUpZswos3NDhQpvmD8-1717573697-1.0.1.1-yVL4P..W5UgHL8BnQkxKaEyK7TFP6nB0fvhGetaf_OVziDkXAsKrstmdpNUUrqJVO3R.Dc6I7E6TsqLfNwH8Hw',
        '_ga_D78WKTMJC6': 'GS1.1.1717573685.5.1.1717573845.0.0.0',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
        'cookie': 'PLAY_SESSION=4f1dfbd690a108c980817e06b7206492613fc6f3-___TS=1717575615993&tahunAnggaranPilihan=2024&___ID=76359e5f-dfb3-4d28-b130-d8cad0a6b51d; _cfuvid=NiCmlRQHkysyvpySU8EpuutYKSqMyluUhLyHya29uvg-1717121340377-0.0.1.1-604800000; _ga=GA1.1.1063132374.1717121341; cf_clearance=U0jRIguPPey.PljlozZbZ3AMzhUpZswos3NDhQpvmD8-1717573697-1.0.1.1-yVL4P..W5UgHL8BnQkxKaEyK7TFP6nB0fvhGetaf_OVziDkXAsKrstmdpNUUrqJVO3R.Dc6I7E6TsqLfNwH8Hw; _ga_D78WKTMJC6=GS1.1.1717573685.5.1.1717573845.0.0.0',
        'priority': 'u=1, i',
        'referer': 'https://sirup.lkpp.go.id/sirup/rekap/penyedia/L32',
        'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'idKldi': 'L32',
        'tahun': '2024',
        'sEcho': '1',
        'iColumns': '8',
        'sColumns': ',,namaPaket,,,,,',
        'iDisplayStart': '0',
        'iDisplayLength': '1000',
        'mDataProp_0': '0',
        'sSearch_0': '',
        'bRegex_0': 'false',
        'bSearchable_0': 'true',
        'bSortable_0': 'false',
        'mDataProp_1': '1',
        'sSearch_1': '',
        'bRegex_1': 'false',
        'bSearchable_1': 'true',
        'bSortable_1': 'true',
        'mDataProp_2': '2',
        'sSearch_2': '',
        'bRegex_2': 'false',
        'bSearchable_2': 'true',
        'bSortable_2': 'true',
        'mDataProp_3': '3',
        'sSearch_3': '',
        'bRegex_3': 'false',
        'bSearchable_3': 'true',
        'bSortable_3': 'true',
        'mDataProp_4': '4',
        'sSearch_4': '',
        'bRegex_4': 'false',
        'bSearchable_4': 'true',
        'bSortable_4': 'true',
        'mDataProp_5': '5',
        'sSearch_5': '',
        'bRegex_5': 'false',
        'bSearchable_5': 'true',
        'bSortable_5': 'true',
        'mDataProp_6': '6',
        'sSearch_6': '',
        'bRegex_6': 'false',
        'bSearchable_6': 'true',
        'bSortable_6': 'true',
        'mDataProp_7': '7',
        'sSearch_7': '',
        'bRegex_7': 'false',
        'bSearchable_7': 'true',
        'bSortable_7': 'true',
        'sSearch': '',
        'bRegex': 'false',
        'iSortCol_0': '0',
        'sSortDir_0': 'asc',
        'iSortingCols': '1',
        '_': '1717573845056',
    }

    response = requests.get(
        'https://sirup.lkpp.go.id/sirup/datatablectr/dataruppenyediakldi',
        params=params,
        cookies=cookies,
        headers=headers,
    )

    def start_requests(self):
        url = "https://sirup.lkpp.go.id/sirup/datatablectr/dataruppenyediakldi"
        yield scrapy.FormRequest(
            url,
            formdata=self.params,
            cookies=self.cookies,
            headers=self.headers,
            callback=self.parse
        )
        

    def parse(self, response):
        data = response.json()
        if 'aaData' in data:    
            for item in data['aaData']:
                id = item[0]
                satuan_kerja = item[1]
                nama_paket = item[2]
                pagu_rp = item[3]
                metode_pemilihan = item[4]
                sumber_dana_api = item[5]
                kode_rup = item[6]
                waktu_pemilihan = item[7]

                detail_url = f'https://sirup.lkpp.go.id/sirup/home/detailPaketPenyediaPublic2017/{id}'
                if detail_url:
                    yield scrapy.Request(detail_url, callback=self.detail_page, meta={
                        'id' : id,
                        'satuan_kerja' : satuan_kerja,
                        'nama_paket' : nama_paket,
                        'pagu_rp' : pagu_rp,
                        'metode_pemilihan' : metode_pemilihan,
                        'sumber_dana_api' : sumber_dana_api,
                        'kode_rup' : kode_rup,
                        'waktu_pemilihan' : waktu_pemilihan,
                    })

    def detail_page(self, response):
        id = response.meta.get('id')
        satuan_kerja = response.meta.get('satuan_kerja')
        nama_paket = response.meta.get('nama_paket')
        pagu_rp = response.meta.get('pagu_rp')
        metode_pemilihan = response.meta.get('metode_pemilihan')
        sumber_dana_api = response.meta.get('sumber_dana_api')
        kode_rup = response.meta.get('kode_rup')
        waktu_pemilihan = response.meta.get('waktu_pemilihan')

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

        pemanfaatan_barang_jasa_table = response.css('td.label-left:contains("Pemanfaatan Barang/Jasa") + td table tr')
        pemanfaatan_barang_jasa = []
        for row in pemanfaatan_barang_jasa_table:
            mulai = row.css('td.mid:nth-child(1)::text').get()
            akhir = row.css('td.mid:nth-child(2)::text').get()
            if all([mulai, akhir]):
                pemanfaatan_barang_jasa.append({
                    'Mulai': mulai,
                    'Akhir': akhir
                })

        detail_data['Pemanfaatan Barang/Jasa'] = pemanfaatan_barang_jasa
        


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


        jadwal_pemilihan_penyedia_table = response.css('td.label-left:contains("Jadwal Pemilihan Penyedia") + td table tr')
        jadwal_pemilihan_penyedia = []
        for row in jadwal_pemilihan_penyedia_table:
            mulai = row.css('td.mid:nth-child(1)::text').get()
            akhir = row.css('td.mid:nth-child(2)::text').get()
            if all([mulai, akhir]):
                jadwal_pemilihan_penyedia.append({
                    'Mulai': mulai,
                    'Akhir': akhir
                })

        detail_data['Jadwal Pemilihan Penyedia'] = jadwal_pemilihan_penyedia

        filename = f'{id}.json'
        local_path = f'D:/Visual Studio Code/Project Done/Magang Github/API/rencana_umum_pengadaan_kppu/rencana_umum_pengadaan_kppu/data/{filename}'
        path_s3 = f's3://ai-pipeline-raw-data/data/data_descriptive/kppu/e-procurement/rencana_umum_pengadaan_kppu/penyedia/json/{filename}'
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
            'metode_pemilihan' : metode_pemilihan,
            'sumber_dana' : sumber_dana_api,
            'kode_rup' : kode_rup,
            'waktu_pemilihan' : waktu_pemilihan,
            'detail_data' : detail_data
        }
        save_json(data, filename)
        # upload_to_s3(local_path, path_s3.replace('s3://', ''))

