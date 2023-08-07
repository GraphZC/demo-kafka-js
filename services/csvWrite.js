import fs from 'fs';
import path from 'path';

export default function csvWriter(fileName, data) {
  
    // Pushing Object values into array
    let dataCsv = Object.values(data).join(',');
    dataCsv += '\r\n';

    const filePath = path.join(process.cwd(), '../data', fileName);
    fs.appendFile(filePath, dataCsv, (err) => {
        if (err) throw err;
        console.log(`Data appended to ${filePath}`);
    });

}