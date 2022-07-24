from multiprocessing import Pool, cpu_count
from bayes_opt.bayesian_optimization import BayesianOptimization # https://github.com/fmfn/BayesianOptimization
import os
from threading import Thread
from Multioptimizer.DBHandler import DBHandler, DBInstance


class MultiOptimizer(object):
    def __init__(self, func, p_bounds, presetVals, colTypes: list, numThreads, numCpus, initPoints, numIter,
                 storageDir: str, targetName, extra=None):
        """
        p_bounds: dict
        presetVals: dict like p_bounds, except with values instead of tuple ranges
        colTypes: ordered list [(column name, type string), ...]
        extra: Anything you want to pass along to the test without tracking (like training data and whatnot)
        Runs numThreads threads where each thread creates a multiprocessing pool with numCpu workers to run tests.


        """
        self.extra = extra
        self.func = func
        self.p_bounds = p_bounds
        self.presetVals = presetVals
        self.numThreads = numThreads
        self.targetName = targetName
        self.colTypes = colTypes
        self.numCpus = int(max(min(numCpus, cpu_count()), 1))
        self.initPoints = initPoints
        self.numIter = numIter
        self.storageDir = os.path.join(storageDir)
        self.dbHandler = DBHandler(self.targetName, self.storageDir, self.colTypes)
        if not os.path.exists(self.storageDir) or not os.path.isdir(self.storageDir):
            os.mkdir(self.storageDir)

    def maximize(self):
        self.run(False)

    def minimize(self):
        self.run(True)

    def run(self, findMin: bool):
        self.processRun(findMin)
        self.generateSummary(findMin)

    def generateSummary(self, findMin, limit=1000):
        """
        limit: The limit on the number of rows of the generated summary csv file
        """

        self.dbHandler.mergeInstances()
        self.dbHandler.generateSummary(self.targetName, findMin, limit, append=False)

    def processRun(self, findMin: bool):
        if self.numCpus > 1:
            pool = Pool(self.numCpus)
            [pool.apply_async(self.threadStarter, args=(findMin, self.dbHandler.newInstance(),)) for i in
             range(self.numCpus)]
            pool.close()
            pool.join()
        else:
            self.threadStarter(findMin, self.dbHandler.newInstance())

    def threadStarter(self, findMin: bool, dbInstance: DBInstance):
        threads = [Thread(target=self.threadRun, args=(findMin, dbInstance)) for i in range(self.numThreads)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        dbInstance.readyToMerge = True

    def threadRun(self, findMin: bool, dbInstance: DBInstance):
        tester = Tester(self.targetName, self.func, self.colTypes, self.presetVals, findMin, dbInstance, self.extra)
        opt = BayesianOptimization(tester.runTest, self.p_bounds)  # random_state=randint(1, int(2 ** 32 - 2)))
        opt.maximize(self.initPoints, self.numIter)
        dbInstance.readyToMerge = True


class Tester(object):
    def __init__(self, targetName, func, colTypes, presetVals, findMin, dbInstance: DBInstance, extra):
        self.colTypes = colTypes
        self.extra = extra
        self.targetName = targetName
        self.func = func
        self.presetVals = presetVals
        self.dbInstance = dbInstance
        self.findMin = findMin

    def runTest(self, **kwargs):
        newDict = {val: kwargs[val] for val in kwargs}
        for key in self.presetVals:
            newDict[key] = self.presetVals[key]
        newDict['extra'] = self.extra
        # Cast types like ints and stuff then update the parameter tuning repo
        castDict = {"TEXT": str, "REAL": float, "INTEGER": int}
        for key, castType in self.colTypes:
            newDict[key] = castDict[castType.replace(" ", "").upper()](newDict[key])
        result = self.func(newDict)

        optResult = None
        for res in result:
            optResult = res[self.targetName]
            if self.findMin:
                if res == 0:
                    optResult = float('inf')
                else:
                    optResult = 1 / optResult
            else:
                optResult = result
            dfDict = newDict.copy()
            for key in res:
                dfDict[key] = res[key]
            self.dbInstance.addRowToDB(dfDict)

        return optResult
