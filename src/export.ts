import { parse } from 'csv-parse';
import fs from 'fs';
import path from 'path';

const WORKER_COUNT = 32;
const delimiter = ';';

function getDataPath(fileName: string): string {
  return path.join('./data/', fileName);
}

function getStaticApiPath(fileName: string): string {
  return path.join('./static/api/', fileName);
}

interface IProvince {
  id: string;
  name: string;
}

interface IRegency {
  id: string;
  province_id: string;
  name: string;
}

interface IDistrict {
  id: string;
  regency_id: string;
  name: string;
}

interface IVillage {
  id: string;
  district_id: string;
  name: string;
}

async function getDataProvince(): Promise<IProvince[]> {
  return new Promise((resolve, reject) => {
    const file = getDataPath('provinces.csv');
    const buffer = fs.readFileSync(file, 'utf8');
    const parser = parse(buffer, { delimiter });
    const data: IProvince[] = [];
    parser.on('readable', () => {
      let record: unknown[];
      while ((record = parser.read())) {
        const [id, name] = record;
        data.push({ id: id as string, name: name as string });
      }
      resolve(data);
    });
  });
}

async function getDataRegency(): Promise<IRegency[]> {
  return new Promise((resolve, reject) => {
    const file = getDataPath('regencies.csv');
    const buffer = fs.readFileSync(file, 'utf8');
    const parser = parse(buffer, { delimiter });
    const data: IRegency[] = [];
    parser.on('readable', () => {
      let record: unknown[];
      while ((record = parser.read())) {
        const [id, province_id, name] = record;
        data.push({
          id: id as string,
          province_id: province_id as string,
          name: name as string,
        });
      }
      resolve(data);
    });
  });
}

async function getDataDistrict(): Promise<IDistrict[]> {
  return new Promise((resolve, reject) => {
    const file = getDataPath('districts.csv');
    const buffer = fs.readFileSync(file, 'utf8');
    const parser = parse(buffer, { delimiter });
    const data: IDistrict[] = [];
    parser.on('readable', () => {
      let record: unknown[];
      while ((record = parser.read())) {
        const [id, regency_id, name] = record;
        data.push({
          id: id as string,
          regency_id: regency_id as string,
          name: name as string,
        });
      }
      resolve(data);
    });
  });
}

async function getDataVillage(): Promise<IVillage[]> {
  return new Promise((resolve, reject) => {
    const file = getDataPath('villages.csv');
    const buffer = fs.readFileSync(file, 'utf8');
    const parser = parse(buffer, { delimiter });
    const data: IVillage[] = [];
    parser.on('readable', () => {
      let record: unknown[];
      while ((record = parser.read())) {
        const [id, district_id, name] = record;
        data.push({
          id: id as string,
          district_id: district_id as string,
          name: name as string,
        });
      }
      resolve(data);
    });
  });
}


async function main(): Promise<void> {
  console.log('Reading data...');
  const provinces = await getDataProvince();
  const regencies = await getDataRegency();
  const districts = await getDataDistrict();
  const villages = await getDataVillage();

  const provincesJson = JSON.stringify(provinces);
  const provincesJsonFilename = getStaticApiPath('provinces.json');
  console.log(`Writing "provinces.json"...`);
  fs.writeFileSync(provincesJsonFilename, provincesJson);
  console.log(`Successfully wrote "provinces.json".`);

  console.log(`Writing "province/*.json"...`);
  const provinceWorker: Promise<void>[] = [];
  for(const province of provinces) {
    if(provinceWorker.length >= WORKER_COUNT){
      await Promise.all(provinceWorker);
      provinceWorker.length = 0;
    }
    const provinceJsonFilename = getStaticApiPath(`province/${province.id}.json`);
    const worker = new Promise<void>((resolve, reject) => {
      const provinceFiltered = provinces.find((_province) => province.id === _province.id);
      fs.writeFile(provinceJsonFilename, JSON.stringify(provinceFiltered), (err) => {
        if(err){
          console.log(`Error writing "province/${province.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    provinceWorker.push(worker);
  }
  await Promise.all(provinceWorker);
  console.log(`Successfully wrote "province/*.json".`);

  console.log(`Writing "regencies/*.json"...`);
  const regenciesWorker: Promise<void>[] = [];
  for(const province of provinces) {
    if(regenciesWorker.length >= WORKER_COUNT){
      await Promise.all(regenciesWorker);
      regenciesWorker.length = 0;
    }
    const regenciesJsonFilename = getStaticApiPath(`regencies/${province.id}.json`);
    const regenciesFiltered = regencies.filter((regency) => regency.province_id === province.id);
    const worker = new Promise<void>((resolve, reject) => {
      fs.writeFile(regenciesJsonFilename, JSON.stringify(regenciesFiltered), (err) => {
        if(err){
          console.log(`Error writing "regencies/${province.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    regenciesWorker.push(worker);
  }
  await Promise.all(regenciesWorker);
  console.log(`Successfully wrote "regencies/*.json".`);
  
  console.log(`Writing "regency/*.json"...`);
  const regencyWorker: Promise<void>[] = [];
  for(const regency of regencies) {
    if(regencyWorker.length >= WORKER_COUNT){
      await Promise.all(regencyWorker);
      regencyWorker.length = 0;
    }
    const regencyJsonFilename = getStaticApiPath(`regency/${regency.id}.json`);
    const worker = new Promise<void>((resolve, reject) => {
      const regencyFiltered = regencies.find((_regency) => regency.id === _regency.id);
      fs.writeFile(regencyJsonFilename, JSON.stringify(regencyFiltered), (err) => {
        if(err){
          console.log(`Error writing "regency/${regency.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    regencyWorker.push(worker);
  }
  await Promise.all(regencyWorker);
  console.log(`Successfully wrote "regency/*.json".`);
  
  console.log(`Writing "districts/*.json"...`);
  const districtsWorker: Promise<void>[] = [];
  for(const regency of regencies) {
    if(districtsWorker.length >= WORKER_COUNT){
      await Promise.all(districtsWorker);
      districtsWorker.length = 0;
    }
    const districtsJsonFilename = getStaticApiPath(`districts/${regency.id}.json`);
    const districtsFiltered = districts.filter((district) => district.regency_id === regency.id);
    const worker = new Promise<void>((resolve, reject) => {
      fs.writeFile(districtsJsonFilename, JSON.stringify(districtsFiltered), (err) => {
        if(err){
          console.log(`Error writing "districts/${regency.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    districtsWorker.push(worker);
  }
  await Promise.all(districtsWorker);
  console.log(`Successfully wrote "districts/*.json".`);
  
  console.log(`Writing "district/*.json"...`);
  const districtWorker: Promise<void>[] = [];
  for(const district of districts) {
    if(districtWorker.length >= WORKER_COUNT){
      await Promise.all(districtWorker);
      districtWorker.length = 0;
    }
    const districtJsonFilename = getStaticApiPath(`district/${district.id}.json`);
    const worker = new Promise<void>((resolve, reject) => {
      const districtFiltered = districts.find((_district) => district.id === _district.id);
      fs.writeFile(districtJsonFilename, JSON.stringify(districtFiltered), (err) => {
        if(err){
          console.log(`Error writing "district/${district.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    districtWorker.push(worker);
  }
  await Promise.all(districtWorker);
  console.log(`Successfully wrote "district/*.json".`);
  
  console.log(`Writing "villages/*.json"...`);
  const villagesWorker: Promise<void>[] = [];
  for(const district of districts) {
    if(villagesWorker.length >= WORKER_COUNT){
      await Promise.all(villagesWorker);
      villagesWorker.length = 0;
    }
    const villagesJsonFilename = getStaticApiPath(`villages/${district.id}.json`);
    const villagesFiltered = villages.filter((village) => village.district_id === district.id);
    const worker = new Promise<void>((resolve, reject) => {
      fs.writeFile(villagesJsonFilename, JSON.stringify(villagesFiltered), (err) => {
        if(err){
          console.log(`Error writing "villages/${district.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    villagesWorker.push(worker);
  }
  await Promise.all(villagesWorker);
  console.log(`Successfully wrote "villages/*.json".`);

  console.log(`Writing "village/*.json"...`);
  const villageWorker: Promise<void>[] = [];
  for(const village of villages) {
    if(villageWorker.length >= WORKER_COUNT){
      await Promise.all(villageWorker);
      villageWorker.length = 0;
    }
    const villageJsonFilename = getStaticApiPath(`village/${village.id}.json`);
    const worker = new Promise<void>((resolve, reject) => {
      const villageFiltered = villages.find((_village) => village.id === _village.id);
      fs.writeFile(villageJsonFilename, JSON.stringify(villageFiltered), (err) => {
        if(err){
          console.log(`Error writing "village/${village.id}.json"!`, err);
          reject();
        } else {
          resolve();
        }
      });
    });
    villageWorker.push(worker);
  }
  await Promise.all(villageWorker);
  console.log(`Successfully wrote "village/*.json".`);

  console.log(``);
  console.log(`Done writing files!`);
}

main();
