import fs from 'node:fs';
import path from 'node:path';

export interface SteelCompanyData {
  company: string;
  year: number;
  production_mt: number | null;
  emissions_mt: number | null;
  weighted_ef: number | null;
  utilization_rate: number | null;
  n_plants: number | null;
  total_capacity_mt: number | null;
  production_source: string;
}

export interface CompanyInfo {
  company: string;
  sector: string;
  country: string;
  ca100_focus: string;
  paris_aligned: string;
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
}

function parseNumber(val: string): number | null {
  if (!val || val === 'NA' || val === 'NaN' || val === '') return null;
  const cleaned = val.replace(/,/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

function loadCSV(relativePath: string): string[][] {
  // Resolve relative to the repo root (website is inside the repo)
  const repoRoot = path.resolve(import.meta.dirname, '..', '..', '..');
  const filePath = path.join(repoRoot, relativePath);

  if (!fs.existsSync(filePath)) {
    console.warn(`CSV not found: ${filePath}`);
    return [];
  }

  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter((l) => l.trim().length > 0);
  return lines.map(parseCSVLine);
}

export function loadSteelAPA(): SteelCompanyData[] {
  const rows = loadCSV('outputs/steel/steel_apa_emissions.csv');
  if (rows.length === 0) return [];

  const headers = rows[0];
  return rows.slice(1).map((row) => {
    const obj: Record<string, string> = {};
    headers.forEach((h, i) => {
      obj[h] = row[i] || '';
    });

    return {
      company: obj['company'] || '',
      year: parseInt(obj['year']) || 0,
      production_mt: parseNumber(obj['production_mt']),
      emissions_mt: parseNumber(obj['emissions_mt']),
      weighted_ef: parseNumber(obj['weighted_ef']),
      utilization_rate: parseNumber(obj['utilization_rate']),
      n_plants: parseNumber(obj['n_plants']),
      total_capacity_mt: parseNumber(obj['total_capacity_mt']),
      production_source: obj['production_source'] || '',
    };
  });
}

export function loadCompanyInfo(): CompanyInfo[] {
  const rows = loadCSV('outputs/steel/steel_company_info.csv');
  if (rows.length === 0) return [];

  const headers = rows[0];
  return rows.slice(1).map((row) => {
    const obj: Record<string, string> = {};
    headers.forEach((h, i) => {
      obj[h] = row[i] || '';
    });
    return {
      company: obj['company'] || '',
      sector: 'Steel',
      country: obj['country'] || '',
      ca100_focus: obj['ca100_focus'] || '',
      paris_aligned: '',
    };
  });
}

export function getUniqueCompanies(data: SteelCompanyData[]): string[] {
  return [...new Set(data.map((d) => d.company))].sort();
}

export function getCompanyTimeSeries(
  data: SteelCompanyData[],
  company: string,
): SteelCompanyData[] {
  return data.filter((d) => d.company === company).sort((a, b) => a.year - b.year);
}
