import fetch from 'node-fetch';
import sqlite from 'sqlite3';
import fs from 'fs';
const db = new sqlite.Database('./build/wilayah.db');

const dbURLSQLfile =
    'https://github.com/cahyadsn/wilayah/raw/master/db/wilayah.sql';

interface IWilayah {
    kode: string;
    nama: string;
}

async function main(): Promise<void> {
    console.log('Downloading database...');
    const response = await fetch(dbURLSQLfile, {
        method: 'GET',
    });
    const sql = await response.text();
    console.log('Database downloaded!');

    console.log('Loading database...');
    await new Promise((resolve, reject) => {
        db.exec(sql, (err) => {
            if (err) {
                reject(err);
            }
            resolve(undefined);
        });
    });

    const wilayahData = await new Promise<IWilayah[]>((resolve, reject) => {
        const array = [] as IWilayah[];
        db.each(
            'SELECT * FROM wilayah;',
            (err, row: IWilayah) => {
                if (err) {
                    reject(err);
                }
                array.push(row);
            },
            (err, count) => {
                if (err) {
                    reject(err);
                }
                resolve(array);
            },
        );
    });
    console.log('Database loaded!');

    console.log('Parsing data...');
    const provincesDataset = [] as string[][];
    const regenciesDataset = [] as string[][];
    const districtsDataset = [] as string[][];
    const villagesDataset = [] as string[][];
    for (const wilayah of wilayahData) {
        const kode = wilayah.kode.split('.');
        const province = kode[0];
        const regency = province + kode[1];
        const district = regency + kode[2];
        const village = district + kode[3];
        switch (kode.length) {
            case 1:                
                provincesDataset.push([province, wilayah.nama.toUpperCase()]);
                break;
            case 2:
                regenciesDataset.push([regency, province, wilayah.nama.toUpperCase().replace("KAB.", "KABUPATEN")]);
                break;
            case 3:
                districtsDataset.push([district, regency, wilayah.nama.toUpperCase()]);
                break;
            case 4:
                villagesDataset.push([village, district, wilayah.nama.toUpperCase()]);
                break;
        }
    }

    const provincesCSV = provincesDataset.map((row) => row.join(';')).join('\n');
    const regenciesCSV = regenciesDataset.map((row) => row.join(';')).join('\n');
    const districtsCSV = districtsDataset.map((row) => row.join(';')).join('\n');
    const villagesCSV = villagesDataset.map((row) => row.join(';')).join('\n');
    console.log('Data parsed!');

    console.log('Writing data...');
    fs.writeFileSync('./data/provinces.csv', provincesCSV);
    fs.writeFileSync('./data/regencies.csv', regenciesCSV);
    fs.writeFileSync('./data/districts.csv', districtsCSV);
    fs.writeFileSync('./data/villages.csv', villagesCSV);
    console.log('Data written!');
}

main();
