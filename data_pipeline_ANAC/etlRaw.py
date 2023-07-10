import os
import pandas as pd

class localTransform():
    def __init__(self, localPath, fromDatabase:str, reportName:str, yearPath:str, formatFile:str):
        self.localPath = localPath
        self.fromDatabase = fromDatabase
        self.reportName = reportName
        self.formatFile = formatFile
        self.yearPath = yearPath

    def gettingFullLocalPath(self):
        fullPath, filesLocal = os.walk(self.localPath), []
        for subPath in fullPath:
            if self.yearPath in subPath[0] if self.reportName in subPath[0] else None if self.fromDatabase in subPath[0] else None:
                for file in subPath[-1]:
                    filesLocal.append(os.path.join(subPath[0],file)) if self.formatFile.upper() in file or self.formatFile.lower() in file else None
        return filesLocal

    @staticmethod
    def fileAnacTransform(file):
        newFile = pd.read_csv(file, sep=';', index_col=False)
        newFile.columns = newFile.columns.str.lower()
        return newFile
    
    def createDataset(self):
        filesList, dataset = self.gettingFullLocalPath(), pd.DataFrame()
        for file in filesList:
            datasetNew = self.fileAnacTransform(file)
            dataset = pd.concat([dataset, datasetNew])
        return dataset