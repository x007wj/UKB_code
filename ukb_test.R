library(tidyverse)
library(lubridate)

library(data.table)   #可以多线程
library(VIM)
library(mice)
library(job)


setwd("~/Desktop/ukb")   #读取wd路径
load("~/Desktop/ukb/ukb_test.RData")          #读取环境

save.image("~/Desktop/ukb/ukb_test.RData")      #保存环境


#读取文件并显示所用时间
time.start <- Sys.time()


job::job(title = "raw1",{
  ukb_test <- as.data.frame(read_csv("/Users/wanjiang/Desktop/ukb/ukb_data/ukb_test.csv"))
})
job::job(title = "raw2",{
  ukb_raw2 <- as.data.frame(read_csv("/Users/wanjiang/Desktop/ukb/ukb_data/ukb51584.csv"))
})
job::job(title = "raw3",{
  ukb_raw3 <- as.data.frame(read_csv("/Users/wanjiang/Desktop/ukb/ukb_data/ukb668900.csv"))
})

time.end <- Sys.time()
time.running <- time.end-time.start
print(time.running)
rm(time.start,time.end,time.running)




ukb_test <- ukb_test[,-1]                   #原有18293列，去除第一列编号，剩18292列

#查看ukb_test
head(ukb_test)




#------1. 寻找变量------
        #有些变量在提取时可以用 starts_with()



ukb_apply <- ukb_test %>% 
  select(
    ##----1.1 编号、年龄、性别等----
    'eid',#id 编码
    '53-0.0',# Date of attending assessment centre	注册时间
    '53-1.0',
    '53-2.0',
    '53-3.0',

    
    '34-0.0',#Year of birth 出生年份
    '52-0.0',#month of birth 出生月份
    
    '20022-0.0',#Birth weight	出生体重
    '31-0.0', #sex
    '21022-0.0',# age at recruitment
    
    '102-0.0',#Pulse rate  脉搏率
    '4194-0.0',#Pulse rate	心率
    
    ##--1.2 血压----
    '4079-0.0',#Diastolic blood pressure DBP
    '4079-0.1',
    '4079-1.0',
    '4079-1.1',
    '4079-2.0',
    '4079-2.1',
    '4079-3.0',
    '4079-3.1',
    
    '4080-0.0',#Systolic blood pressure  SBP
    '4080-0.1',
    '4080-1.0',
    '4080-1.1',
    '4080-2.0',
    '4080-2.1',
    '4080-3.0',
    '4080-3.1',
    

    ##----1.3 身高、体重和BMI----
    '21001-0.0',#Body mass index         BMI
    '21001-1.0',
    '21001-2.0',
    '21001-3.0',
    
    '23098-0.0',#Weight	体重
    '21002-0.0',#Weight	体重
    
    '50-0.0',#standing height站高
    '48-0.0',#waist circumference 腰围
    '49-0.0',#hip circumference 臀围
    
    ##----1.4 种族----
    '21000-0.0',#Ethnic background	种族
    '21000-1.0',
    '21000-2.0',
    
    ##----1.5 教育----
    '6138-0.0',  #Qualifications 教育 code：100305
    '6138-0.1',
    '6138-0.2',
    '6138-0.3',
    '6138-0.4',
    '6138-0.5',
    '6138-1.0',
    '6138-1.1',
    '6138-1.2',
    '6138-1.3',
    '6138-1.4',
    '6138-1.5',    
    '6138-2.0',
    '6138-2.1',
    '6138-2.2',
    '6138-2.3',
    '6138-2.4',
    '6138-2.5',
    '6138-3.0',
    '6138-3.1',
    '6138-3.2',
    '6138-3.3',
    '6138-3.4',
    '6138-3.5',
    
    '10722-0.0',# Qualifications (pilot)   教育（飞行员？）code:10658
    '10722-0.1',
    '10722-0.2',
    '10722-0.3',
    '10722-0.4',
    
    
    ##-----1.6 diet部分-------
                     #水果
    '1309-0.0',      #	Fresh fruit intake 新鲜水果
    '1309-1.0',
    '1309-2.0',
    '1309-3.0',
    '1319-0.0',      #	Dried fruit intake 干果
    '1319-1.0',  
    '1319-2.0',  
    '1319-3.0',  
                     #蔬菜
    '1289-0.0',      #	Cooked vegetable intake 熟蔬菜
    '1289-1.0', 
    '1289-2.0', 
    '1289-3.0', 
    '1299-0.0',      #	Salad / raw vegetable intake 生蔬菜
    '1299-1.0',
    '1299-2.0',
    '1299-3.0',
    
                     #全谷物/精制谷物
    '1438-0.0',      #	Bread intake  面包
    '1438-1.0', 
    '1438-2.0', 
    '1438-3.0', 
    '1448-0.0',      #  Bread type  面包类型
    '1448-1.0', 
    '1448-2.0', 
    '1448-3.0', 
    '1458-0.0',      #	Cereal intake  麦片
    '1458-1.0',
    '1458-2.0',
    '1458-3.0',
    '1468-0.0',      #  Cereal type  麦片类型
    '1468-1.0', 
    '1468-2.0', 
    '1468-3.0', 
                     #鱼  
    '1329-0.0',      #  Oily fish intake 油性鱼
    '1329-1.0',
    '1329-2.0',
    '1329-3.0',
    '1339-0.0',      #  Non-oily fish intake 非油性鱼
    '1339-1.0', 
    '1339-2.0', 
    '1339-3.0', 
                     #奶制品
    '1408-0.0',      #	Cheese intake  奶酪
    '1408-1.0', 
    '1408-2.0', 
    '1408-3.0', 
    '1418-0.0',      #  Milk type used 牛奶类型
    '1418-1.0',
    '1418-2.0',
    '1418-3.0',
                     #肉类
    '1349-0.0',      #  Processed meat intake 加工肉类
    '1349-1.0',
    '1349-2.0',
    '1349-3.0',
    '3680-0.0',      #  Age when last ate meat  上次吃肉的年龄
    '3680-1.0', 
    '3680-2.0', 
    '3680-3.0', 
    '1359-0.0',      #  Poultry intake 家禽
    '1359-1.0', 
    '1359-2.0', 
    '1359-3.0', 
    '1369-0.0',      #  Beef intake 牛肉
    '1369-1.0', 
    '1369-2.0', 
    '1369-3.0', 
    '1379-0.0',      #  Lamb/mutton intake  羊肉
    '1379-1.0',
    '1379-2.0',
    '1379-3.0',
    '1389-0.0',      #	Pork intake 猪肉
    '1389-1.0',
    '1389-2.0',
    '1389-3.0',
    #加糖饮料
    '6144-0.0',      #  Never eat eggs, dairy, wheat, sugar  从不吃蛋、奶、小麦、糖？
    '6144-1.0', 
    '6144-2.0', 
    '6144-3.0', 
    
    "2654-0.0",# non_butter spread type details
    "2654-1.0",
    "2654-2.0",
    "2654-3.0",
    '1428-0.0',      #  Spread type 涂酱类型
    '1428-1.0',
    '1428-2.0',
    '1428-3.0',
    
    ####------暂时未用到的 饮食
    # '10855-0.0',     #	Never eat eggs, dairy, wheat, sugar (pilot) 从不吃蛋、奶、小麦、糖？飞行员
    # '10767-0.0',     #	Spread type (pilot) 涂酱类型（飞行员）
    # '10776-0.0',     #  Bread type/intake (pilot) 面包类型/摄入
    # '1478-0.0',      #	Salt added to food 添加盐
    # '1488-0.0',      #  Tea intake 茶
    # '1498-0.0',      #  Coffee intake  咖啡 
    # '1508-0.0',      #	Coffee type 咖啡类型 
    # '1518-0.0',      #	Hot drink temperature  热饮温度
    # '1528-0.0',      #  Water intake 水
    # '1538-0.0',      #  Major dietary changes in the last 5 years 过去五年的主要饮食变化
    # '1548-0.0',      #	Variation in diet 饮食变化
    # '10912-0.0',     #	Variation in diet (pilot)  饮食变化（飞行员）
    
    
    
    ##----1.7 体力活动----
    '22032-0.0',#IPAQ         
    '22037-0.0',#met_walking
    '22038-0.0',#met_moderate
    '22039-0.0',#met_vigorous
    
    ##----1.8 睡眠----
    '1160-0.0',#Sleep duration 睡眠时间
    '1160-1.0',
    '1160-2.0',
    '1160-3.0',
    
    '1170-0.0',#Getting up in morning  早起难易程度
    
    ##----1.9 吸烟情况----
    '1239-0.0',#Current tobacco smoking 当前吸烟情况
    '1239-1.0',
    '1239-2.0',
    '1239-3.0',
    '1249-0.0',#Past tobacco smoking  过去吸烟情况
    '1249-1.0',
    '1249-2.0',
    '1249-3.0',
    '2644-0.0',#吸过至少100支烟
    '2644-1.0',
    '2644-2.0',
    '2644-3.0',
    
    '20116-0.0',#Smoking status	吸烟状态  "-3：Prefer not to answer   0：Never  1：Previous  2	Current"
    '20116-1.0',
    '20116-2.0',
    '20116-3.0',
    
    ##----1.10 饮酒情况----
    '20414-0.0',  #Frequency of drinking alcohol   code:521 
    
    '100580-0.0',  #alcohol consumed      code:100010
    '100580-1.0',
    '100580-2.0',
    '100580-3.0',
    '100580-4.0',
    
    '100590-0.0',  #red wine                 code:100006
    '100590-1.0', 
    '100590-2.0', 
    '100590-3.0', 
    '100590-4.0', 
    
    '100630-0.0',  #rose wine
    '100630-1.0',
    '100630-2.0',
    '100630-3.0',
    '100630-4.0',
    
    '100670-0.0',  #white wine
    '100670-1.0',
    '100670-2.0',
    '100670-3.0',
    '100670-4.0',
    
    '100710-0.0',  #beer/cider
    '100710-1.0',
    '100710-2.0',
    '100710-3.0',
    '100710-4.0',
    
    '100720-0.0',  #fortified wine
    '100720-1.0',
    '100720-2.0',
    '100720-3.0',
    '100720-4.0',
    
    '100730-0.0',  #spirits
    '100730-1.0',
    '100730-2.0',
    '100730-3.0',
    '100730-4.0',
    
    '100740-0.0',  #other alcohol
    '100740-1.0',
    '100740-2.0',
    '100740-3.0',
    '100740-4.0',
    
    '20117-0.0',#Alcohol drinker status	饮酒状态"-3：Prefer not to answer   0：Never  1：Previous  2	Current"
    '20117-1.0',
    '20117-2.0',
    '20117-3.0',    
    
    ##----1.11 工作----
    '20119-0.0',#Current employment status - corrected	就业状态
    
    '6142-0.0',  #current employment status
    '6142-0.1',
    '6142-0.2',
    '6142-0.3',
    '6142-0.4',
    '6142-0.5',
    '6142-0.6',
    '6142-1.0',  
    '6142-1.1',
    '6142-1.2',
    '6142-1.3',
    '6142-1.4',
    '6142-1.5',
    '6142-1.6',
    '6142-2.0',  
    '6142-2.1',
    '6142-2.2',
    '6142-2.3',
    '6142-2.4',
    '6142-2.5',
    '6142-2.6',
    '6142-3.0', 
    '6142-3.1',
    '6142-3.2',
    '6142-3.3',
    '6142-3.4',
    '6142-3.5',
    '6142-3.6',
    
    ##----1.12 家庭收入----
    '738-0.0',             #平均税前家庭总收入 code :100294
    '738-1.0', 
    '738-2.0', 
    '738-3.0', 
    
    '10877-0.0',#   平均税前家庭总收入(pilot) code :100657
    
    ##----1.13 实验室指标----
    '30630-0.0',#载脂蛋白A
    '30630-1.0',#
    '30640-0.0',#载脂蛋白B
    '30640-1.0',
    '30690-0.0',#胆固醇
    '30690-1.0',
    '30870-0.0',#TC-甘油三酯
    '30870-1.0',
    '30760-0.0',#HDL
    '30760-1.0',
    '30780-0.0',#LDL
    '30780-1.0',
    '30790-0.0',#脂蛋白A
    '30790-1.0',
    
    '30740-0.0',#血糖1
    '30740-1.0',#血糖2
    '30750-0.0',#糖化血红蛋白1
    '30750-1.0',#糖化血红蛋白2
    
    '30700-0.0',#肌酐
    '30710-0.0',#C反应蛋白
    '30730-0.0',#γ-谷氨酰转移酶
    '30800-0.0',#雌二醇
    '30810-0.0',#磷酸盐
    '30820-0.0',#类风湿因子
    '30830-0.0',#SHBG-性激素结合球蛋白
    '30840-0.0',#总胆红素
    '30850-0.0',#睾酮
    '30860-0.0',#总蛋白
    '30880-0.0',#尿酸盐
    '30890-0.0',#Vitamin D
    
    
    ##----1.14 用药情况----
    '6153-0.0',#Medication for cholesterol, blood pressure, diabetes, or take exogenous hormones	治疗胆固醇、血压、糖尿病、或服用外源性激素的药物
    '6153-0.1',
    '6153-0.2',
    '6153-0.3',
    '6153-1.0',
    '6153-1.1',
    '6153-1.2',
    '6153-1.3', 
    '6153-2.0',
    '6153-2.1',
    '6153-2.2',
    '6153-2.3', 
    '6153-3.0',
    '6153-3.1',
    '6153-3.2',
    '6153-3.3',  
    
    '6177-0.0',#Medication for cholesterol, blood pressure or diabetes	治疗胆固醇、血压或糖尿病的药物
    '6177-0.1',    
    '6177-0.2',
    '6177-1.0',    
    '6177-1.1',
    '6177-1.2',
    '6177-2.0',   
    '6177-2.1',
    '6177-2.2',
    '6177-3.0',    
    '6177-3.1',
    '6177-3.2',    
    
    '20003-0.0',#Treatment/medication code	治疗、药物编码：4
    
    ##----1.15 糖尿病诊断----
    '2443-0.0',#Diabetes diagnosed by doctor	糖尿病诊断
    '2443-1.0',
    '2443-2.0',
    '2443-3.0',
    
    ##----1.16  自报的癌症----
    #medical conditions
    '20001-0.0',  #  cancer code, self reported
    '20001-0.1',
    '20001-0.2',
    '20001-0.3',
    '20001-0.4',
    '20001-0.5',
    '20001-1.0',  #  1
    '20001-1.1',
    '20001-1.2',
    '20001-1.3',
    '20001-1.4',
    '20001-1.5',
    '20001-2.0',  #  2
    '20001-2.1',
    '20001-2.2',
    '20001-2.3',
    '20001-2.4',
    '20001-2.5',
    '20001-3.0',  #  3
    '20001-3.1',
    '20001-3.2',
    '20001-3.3',
    '20001-3.4',
    '20001-3.5',
    
    '134-0.0',  #number of self-reported cancers
    '134-1.0',
    '134-2.0',
    '134-3.0',
    
    ##----1.17 自报的非癌疾病----
    #non-cancer illness code, self reported
    '20002-0.0','20002-0.1','20002-0.2','20002-0.3','20002-0.4','20002-0.5','20002-0.6','20002-0.7','20002-0.8','20002-0.9',
    '20002-0.10','20002-0.11','20002-0.12','20002-0.13','20002-0.14','20002-0.15','20002-0.16','20002-0.17','20002-0.18','20002-0.19',
    '20002-0.20','20002-0.21','20002-0.22','20002-0.23','20002-0.24','20002-0.25','20002-0.26','20002-0.27','20002-0.28','20002-0.29',
    '20002-0.30','20002-0.31','20002-0.32','20002-0.33',
    
    '20002-1.0','20002-1.1','20002-1.2','20002-1.3','20002-1.4','20002-1.5','20002-1.6','20002-1.7','20002-1.8','20002-1.9',
    '20002-1.10','20002-1.11','20002-1.12','20002-1.13','20002-1.14','20002-1.15','20002-1.16','20002-1.17','20002-1.18','20002-1.19',
    '20002-1.20','20002-1.21','20002-1.22','20002-1.23','20002-1.24','20002-1.25','20002-1.26','20002-1.27','20002-1.28','20002-1.29',
    '20002-1.30','20002-1.31','20002-1.32','20002-1.33',
    
    '20002-2.0','20002-2.1','20002-2.2','20002-2.3','20002-2.4','20002-2.5','20002-2.6','20002-2.7','20002-2.8','20002-2.9',
    '20002-2.10','20002-2.11','20002-2.12','20002-2.13','20002-2.14','20002-2.15','20002-2.16','20002-2.17','20002-2.18','20002-2.19',
    '20002-2.20','20002-2.21','20002-2.22','20002-2.23','20002-2.24','20002-2.25','20002-2.26','20002-2.27','20002-2.28','20002-2.29',
    '20002-2.30','20002-2.31','20002-2.32','20002-2.33',
    
    '20002-3.0','20002-3.1','20002-3.2','20002-3.3','20002-3.4','20002-3.5','20002-3.6','20002-3.7','20002-3.8','20002-3.9',
    '20002-3.10','20002-3.11','20002-3.12','20002-3.13','20002-3.14','20002-3.15','20002-3.16','20002-3.17','20002-3.18','20002-3.19',
    '20002-3.20','20002-3.21','20002-3.22','20002-3.23','20002-3.24','20002-3.25','20002-3.26','20002-3.27','20002-3.28','20002-3.29',
    '20002-3.30','20002-3.31','20002-3.32','20002-3.33',
    
    '135-0.0',  #number of self-reported non-cancer illnesses
    '135-1.0', 
    '135-2.0', 
    '135-3.0', 
    
    # '84-0.0', #cancer year/age first occurred  
    # '84-0.1', 
    # '84-0.2', 
    # '84-0.3', 
    # '84-0.4', 
    # '84-0.5', 
    # '84-1.0', #1
    # '84-1.1', 
    # '84-1.2', 
    # '84-1.3', 
    # '84-1.4', 
    # '84-1.5', 
    # '84-2.0', #2 
    # '84-2.1', 
    # '84-2.2', 
    # '84-2.3', 
    # '84-2.4', 
    # '84-2.5', 
    # '84-2.0', #3 
    # '84-3.1', 
    # '84-3.2', 
    # '84-3.3', 
    # '84-3.4', 
    # '84-3.5', 
    
    # # Non-cancer illness year/age first occurred
    # '87-0.0','87-0.1','87-0.2','87-0.3','87-0.4','87-0.5','87-0.6','87-0.7','87-0.8','87-0.9',
    # '87-0.10','87-0.11','87-0.12','87-0.13','87-0.14','87-0.15','87-0.16','87-0.17','87-0.18','87-0.19',
    # '87-0.20','87-0.21','87-0.22','87-0.23','87-0.24','87-0.25','87-0.26','87-0.27','87-0.28','87-0.29',
    # '87-0.30','87-0.31','87-0.32','87-0.33',
    # 
    # '87-1.0','87-1.1','87-1.2','87-1.3','87-1.4','87-1.5','87-1.6','87-1.7','87-1.8','87-1.9',
    # '87-1.10','87-1.11','87-1.12','87-1.13','87-1.14','87-1.15','87-1.16','87-1.17','87-1.18','87-1.19',
    # '87-1.20','87-1.21','87-1.22','87-1.23','87-1.24','87-1.25','87-1.26','87-1.27','87-1.28','87-1.29',
    # '87-1.30','87-1.31','87-1.32','87-1.33',
    # 
    # '87-2.0','87-2.1','87-2.2','87-2.3','87-2.4','87-2.5','87-2.6','87-2.7','87-2.8','87-2.9',
    # '87-2.10','87-2.11','87-2.12','87-2.13','87-2.14','87-2.15','87-2.16','87-2.17','87-2.18','87-2.19',
    # '87-2.20','87-2.21','87-2.22','87-2.23','87-2.24','87-2.25','87-2.26','87-2.27','87-2.28','87-2.29',
    # '87-2.30','87-2.31','87-2.32','87-2.33',
    # 
    # '87-3.0','87-3.1','87-3.2','87-3.3','87-3.4','87-3.5','87-3.6','87-3.7','87-3.8','87-3.9',
    # '87-3.10','87-3.11','87-3.12','87-3.13','87-3.14','87-3.15','87-3.16','87-3.17','87-3.18','87-3.19',
    # '87-3.20','87-3.21','87-3.22','87-3.23','87-3.24','87-3.25','87-3.26','87-3.27','87-3.28','87-3.29',
    # '87-3.30','87-3.31','87-3.32','87-3.33',

    ##----1.18 家族史----
    
    # "20112-0.0",# Illnesses of adopted father  0-3*0-6
    # "20113-0.0",# Illnesses of adopted mother
    # "20114-0.0",# Illnesses of adopted siblings
    
    "20107-0.0",# Illnesses of  father  code:1010
    "20107-0.1",
    "20107-0.2",
    "20107-0.3",
    "20107-0.4",
    "20107-0.5",
    "20107-0.6",
    "20107-0.7",
    "20107-0.8",
    "20107-0.9",
    "20107-1.0",#1
    "20107-1.1",
    "20107-1.2",
    "20107-1.3",
    "20107-1.4",
    "20107-1.5",
    "20107-1.6",
    "20107-1.7",
    "20107-1.8",
    "20107-1.9",
    "20107-2.0",#2
    "20107-2.1",
    "20107-2.2",
    "20107-2.3",
    "20107-2.4",
    "20107-2.5",
    "20107-2.6",
    "20107-2.7",
    "20107-2.8",
    "20107-2.9",
    "20107-3.0",#3
    "20107-3.1",
    "20107-3.2",
    "20107-3.3",
    "20107-3.4",
    "20107-3.5",
    "20107-3.6",
    "20107-3.7",
    "20107-3.8",
    "20107-3.9",
    
    "20110-0.0",# Illnesses of  mother
    "20110-0.1",
    "20110-0.2",
    "20110-0.3",
    "20110-0.4",
    "20110-0.5",
    "20110-0.6",
    "20110-0.7",
    "20110-0.8",
    "20110-0.9",
    "20110-0.10",
    "20110-1.0",#1
    "20110-1.1",
    "20110-1.2",
    "20110-1.3",
    "20110-1.4",
    "20110-1.5",
    "20110-1.6",
    "20110-1.7",
    "20110-1.8",
    "20110-1.9",
    "20110-1.10",   
    "20110-2.0",#2
    "20110-2.1",
    "20110-2.2",
    "20110-2.3",
    "20110-2.4",
    "20110-2.5",
    "20110-2.6",
    "20110-2.7",
    "20110-2.8",
    "20110-2.9",
    "20110-2.10", 
    "20110-3.0",#3
    "20110-3.1",
    "20110-3.2",
    "20110-3.3",
    "20110-3.4",
    "20110-3.5",
    "20110-3.6",
    "20110-3.7",
    "20110-3.8",
    "20110-3.9",
    "20110-3.10", 

    "20111-0.0",# Illnesses of  siblings
    "20111-0.1",
    "20111-0.2",
    "20111-0.3",
    "20111-0.4",
    "20111-0.5",
    "20111-0.6",
    "20111-0.7",
    "20111-0.8",
    "20111-0.9",
    "20111-0.10",
    "20111-0.11",
    "20111-1.0",# 1
    "20111-1.1",
    "20111-1.2",
    "20111-1.3",
    "20111-1.4",
    "20111-1.5",
    "20111-1.6",
    "20111-1.7",
    "20111-1.8",
    "20111-1.9",
    "20111-1.10",
    "20111-1.11",
    "20111-2.0",# 2
    "20111-2.1",
    "20111-2.2",
    "20111-2.3",
    "20111-2.4",
    "20111-2.5",
    "20111-2.6",
    "20111-2.7",
    "20111-2.8",
    "20111-2.9",
    "20111-2.10",
    "20111-2.11",
    "20111-3.0",# 3
    "20111-3.1",
    "20111-3.2",
    "20111-3.3",
    "20111-3.4",
    "20111-3.5",
    "20111-3.6",
    "20111-3.7",
    "20111-3.8",
    "20111-3.9",
    "20111-3.10",
    "20111-3.11",
    
   ##----1.19 主要死亡结局----
    '40000-0.0',#Date of death	死亡日期
    '40000-1.0',
  
    '40001-0.0',#Underlying (primary) cause of death: ICD10	主要死亡原因 ICD-10  code:19
    '40001-1.0',  
  
   #----次要死因--暂时不用
   # starts_with("40002-"),#Contributory (secondary) causes of death: ICD10	次要死亡原因 ICD-10  code:19
   
  
   ##----1.20 发病结局----
  #直接从原始数据中读取  这里不再导入
  # starts_with("41270-"),# Diagnoses - ICD10	诊断 ICD-10  code:19
  # starts_with("41280-"),#Date of first in-patient diagnosis - ICD10	首次住院诊断日期
  # 
  # starts_with("41271-"),# Diagnoses - ICD9
  # starts_with("41281-"),#Date of first in-patient diagnosis - ICD9 首次住院诊断日期
  # 
  # starts_with("41202-"),#Diagnoses - main ICD10	诊断 ICD-10
  # starts_with("41262-"),#Date of first in-patient diagnosis - main ICD10
  # 
  # starts_with("41203-"),#main ICD9
  # starts_with("41263-"),#Date of first in-patient diagnosis - main ICD9
  # 
  #次级诊断-----暂时不用？
    # '41204-0.0',#Diagnoses - secondary ICD10	诊断 ICD-10 二级
    # '41205-0.0',#secondary ICD9
    # 
  
  
  ##----1.2？ 其他（）----
  
  '20118-0.0',#Home area population density - urban or rural	居住地人口密度，城市/农村
  
  '709-0.0',#number in household 家庭成员数   
  
  '6150-0.0',#Vascular/heart problems diagnosed by doctor	医生诊断的心脏问题
  
  
    
    '2453-0.0',#Cancer diagnosed by doctor	  癌症诊断
    '2724-0.0',#Had menopause	                绝经
    '3894-0.0',#Age heart attack diagnosed	  心脏病发作的年龄
    '4022-0.0',#Age pulmonary embolism (blood clot in lung) diagnosed	诊断肺栓塞的年龄
    '4056-0.0',#Age stroke diagnosed        	诊断卒中的年龄
    '135-0.0',#Number of self-reported non-cancer illnesses	自报非癌病数
    '20002-0.0',#Non-cancer illness code, self-reported	自报非癌病编码，code：6
    '20008-0.0',#Interpolated Year when non-cancer illness first diagnosed	首次诊断出非癌疾病的年份
    '20009-0.0',#Interpolated Age of participant when non-cancer illness first diagnosed	首次诊断出非癌疾病的年龄
    
    '191-0.0',#Date lost to follow-up	失访日期
    

    

    '42000-0.0',#Date of myocardial infarction	MI日期
    '42002-0.0',#Date of STEMI	STEMI日期
    '42004-0.0',#Date of NSTEMI	NSTEMI日期
    '42006-0.0',#Date of stroke	Stroke日期
    '42008-0.0',#Date of ischaemic stroke	I - stroke日期
    '42010-0.0',#Date of intracerebral haemorrhage	脑出血
    '42012-0.0',#Date of subarachnoid haemorrhage	蛛网膜下腔出血
    '42026-0.0',#Date of end stage renal disease report	终末期肾病
    
    '100001-0.0',#Food weight	食物重量
    '100002-0.0',#Energy	能量
    '100003-0.0',#Protein	蛋白质
    '100004-0.0',#Fat	脂肪
    '100005-0.0',#Carbohydrate	糖
    '100006-0.0',#Saturated fat	饱和脂肪
    '100007-0.0',#Polyunsaturated fat	多不饱和脂肪
    '100008-0.0',#Total sugars	总糖
    '100009-0.0',#Englyst dietary fibre	膳食纤维
  ) %>% 
  
  
  #----2. 重命名变量----
  rename(
    ##----2.1 编号、年龄、性别等----
    date_bl_1='53-0.0',# Date of attending assessment centre	注册时间
    date_bl_2='53-1.0',
    date_bl_3='53-2.0',
    date_bl_4='53-3.0',

    birth_year='34-0.0',#Year of birth 出生年份
    birth_month='52-0.0',#month of birth 出生月份
    age_recruitment='21022-0.0',# age at recruitment
    
    
    birth_weight='20022-0.0',#Birth weight	出生体重
    sex='31-0.0', #sex  性别

    pulse_rate='102-0.0',#Pulse rate  脉搏率
    heart_rate='4194-0.0',#Pulse rate	心率
    
    ##--2.2 血压----
    dbp00='4079-0.0',#Diastolic blood pressure DBP
    dbp01='4079-0.1',
    dbp10='4079-1.0',
    dbp11='4079-1.1',
    dbp20='4079-2.0',
    dbp21='4079-2.1',
    dbp30='4079-3.0',
    dbp31='4079-3.1',
    
    sbp00='4080-0.0',#Systolic blood pressure  SBP
    sbp01='4080-0.1',
    sbp10='4080-1.0',
    sbp11='4080-1.1',
    sbp20='4080-2.0',
    sbp21='4080-2.1',
    sbp30='4080-3.0',
    sbp31='4080-3.1',

    
    ##----2.3 身高、体重和BMI----
    bmi_1='21001-0.0',#Body mass index         BMI
    bmi_2='21001-1.0',
    bmi_3='21001-2.0',
    bmi_4='21001-3.0',
    
    weight='23098-0.0',#Weight	体重
    
    height='50-0.0',#standing height站高
    waist_c='48-0.0',#waist circumference 腰围
    hip_c='49-0.0',#hip circumference 臀围
    
    
    ##----2.4 种族----
    race_1='21000-0.0',#Ethnic background	种族
    race_2='21000-1.0',
    race_3='21000-2.0',   
    
    
    ##----2.5 教育----
    edu_00='6138-0.0',  #Qualifications 教育 code：100305
    edu_01='6138-0.1',
    edu_02='6138-0.2',
    edu_03='6138-0.3',
    edu_04='6138-0.4',
    edu_05='6138-0.5',
    edu_10='6138-1.0',
    edu_11='6138-1.1',
    edu_12='6138-1.2',
    edu_13='6138-1.3',
    edu_14='6138-1.4',
    edu_15='6138-1.5',    
    edu_20='6138-2.0',
    edu_21='6138-2.1',
    edu_22='6138-2.2',
    edu_23='6138-2.3',
    edu_24='6138-2.4',
    edu_25='6138-2.5',
    edu_30='6138-3.0',
    edu_31='6138-3.1',
    edu_32='6138-3.2',
    edu_33='6138-3.3',
    edu_34='6138-3.4',
    edu_35='6138-3.5',
    
    edu_p0='10722-0.0',# Qualifications (pilot)   教育（飞行员？）code:10658
    edu_p1='10722-0.1',
    edu_p2='10722-0.2',
    edu_p3='10722-0.3',
    edu_p4='10722-0.4',    

    
    ##-----2.6 diet部分-------
    #水果
    fresh_fruit_1='1309-0.0',      #	Fresh fruit intake 新鲜水果
    fresh_fruit_2='1309-1.0',
    fresh_fruit_3='1309-2.0',
    fresh_fruit_4='1309-3.0',
    dried_fruit_1='1319-0.0',      #	Dried fruit intake 干果
    dried_fruit_2='1319-1.0',  
    dried_fruit_3='1319-2.0',  
    dried_fruit_4='1319-3.0',  
    #蔬菜
    cooked_vegetable_1='1289-0.0',      #	Cooked vegetable intake 熟蔬菜
    cooked_vegetable_2='1289-1.0', 
    cooked_vegetable_3='1289-2.0', 
    cooked_vegetable_4='1289-3.0', 
    raw_vegetable_1='1299-0.0',      #	Salad / raw vegetable intake 生蔬菜
    raw_vegetable_2='1299-1.0',
    raw_vegetable_3='1299-2.0',
    raw_vegetable_4='1299-3.0',
    
    #全谷物/精制谷物
    bread_intake_1='1438-0.0',      #	Bread intake  面包
    bread_intake_2='1438-1.0', 
    bread_intake_3='1438-2.0', 
    bread_intake_4='1438-3.0', 
    bread_type_1='1448-0.0',      #  Bread type  面包类型
    bread_type_2='1448-1.0', 
    bread_type_3='1448-2.0', 
    bread_type_4='1448-3.0', 
    cereal_intake_1='1458-0.0',      #	Cereal intake  麦片
    cereal_intake_2='1458-1.0',
    cereal_intake_3='1458-2.0',
    cereal_intake_4='1458-3.0',
    cereal_type_1='1468-0.0',      #  Cereal type  麦片类型
    cereal_type_2='1468-1.0', 
    cereal_type_3='1468-2.0', 
    cereal_type_4='1468-3.0', 
    #鱼  
    oily_fish_1='1329-0.0',      #  Oily fish intake 油性鱼
    oily_fish_2='1329-1.0',
    oily_fish_3= '1329-2.0',
    oily_fish_4='1329-3.0',
    non_oily_fish_1='1339-0.0',      #  Non-oily fish intake 非油性鱼
    non_oily_fish_2='1339-1.0', 
    non_oily_fish_3='1339-2.0', 
    non_oily_fish_4='1339-3.0', 
    #奶制品
    cheese_intake_1='1408-0.0',      #	Cheese intake  奶酪
    cheese_intake_2='1408-1.0', 
    cheese_intake_3='1408-2.0', 
    cheese_intake_4= '1408-3.0', 
    milk_type_1='1418-0.0',      #  Milk type used 牛奶类型
    milk_type_2='1418-1.0',
    milk_type_3='1418-2.0',
    milk_type_4='1418-3.0',
    #肉类
    processed_meat_1='1349-0.0',      #  Processed meat intake 加工肉类
    processed_meat_2='1349-1.0',
    processed_meat_3='1349-2.0',
    processed_meat_4='1349-3.0',
    age_meat_1='3680-0.0',      #  Age when last ate meat  上次吃肉的年龄
    age_meat_2='3680-1.0', 
    age_meat_3='3680-2.0', 
    age_meat_4='3680-3.0', 
    poultry_1='1359-0.0',      #  Poultry intake 家禽
    poultry_2='1359-1.0', 
    poultry_3='1359-2.0', 
    poultry_4='1359-3.0', 
    beef_1='1369-0.0',      #  Beef intake 牛肉
    beef_2='1369-1.0', 
    beef_3='1369-2.0', 
    beef_4='1369-3.0', 
    lamb_1='1379-0.0',      #  Lamb/mutton intake  羊肉
    lamb_2='1379-1.0',
    lamb_3='1379-2.0',
    lamb_4='1379-3.0',
    pork_1='1389-0.0',      #	Pork intake 猪肉
    pork_2='1389-1.0',
    pork_3='1389-2.0',
    pork_4='1389-3.0',
    #加糖饮料
    ssb_1='6144-0.0',      #  Never eat eggs, dairy, wheat, sugar  从不吃蛋、奶、小麦、糖？
    ssb_2='6144-1.0', 
    ssb_3='6144-2.0', 
    ssb_4='6144-3.0', 
    
    non_butter_1="2654-0.0", # Non-butter spread type details
    non_butter_2="2654-1.0",
    non_butter_3="2654-2.0",
    non_butter_4="2654-3.0",
    
    spread_type_1='1428-0.0',      #  Spread type 涂酱类型
    spread_type_2='1428-1.0',
    spread_type_3='1428-2.0',
    spread_type_4= '1428-3.0',
    
    
    ##----2.7 体力活动----
    IPAQ='22032-0.0',#IPAQ         ###----体力活动
    met_walking='22037-0.0',#met_walking
    met_moderate='22038-0.0',#met_moderate
    met_vigorous='22039-0.0',#met_vigorous
    
    
    ##----2.8 睡眠----
    sleep_duration_1='1160-0.0',#Sleep duration 睡眠时间
    sleep_duration_2='1160-1.0',
    sleep_duration_3='1160-2.0',
    sleep_duration_4='1160-3.0',
    
    get_up='1170-0.0',#Getting up in morning  早起难易程度
    
    
    ##----2.9 吸烟情况----
    c_smoke_1='1239-0.0',#Current tobacco smoking 当前吸烟情况
    c_smoke_2='1239-1.0',
    c_smoke_3='1239-2.0',
    c_smoke_4='1239-3.0',
    p_smoke_1='1249-0.0',#Past tobacco smoking  过去吸烟情况
    p_smoke_2='1249-1.0',
    p_smoke_3='1249-2.0',
    p_smoke_4='1249-3.0',
    smoke100_1='2644-0.0',#吸过至少100支烟
    smoke100_2='2644-1.0',
    smoke100_3='2644-2.0',
    smoke100_4='2644-3.0',
    
    smoke_status_1='20116-0.0',#Smoking status	吸烟状态  "-3：Prefer not to answer   0：Never  1：Previous  2	Current"
    smoke_status_2='20116-1.0',
    smoke_status_3='20116-2.0',
    smoke_status_4='20116-3.0',
    
    
    ##----2.10 饮酒情况----
    freq_drink='20414-0.0',  #Frequency of drinking alcohol   code:521 
    
    alcohol_consumed_1='100580-0.0',  #alcohol consumed      code:100010
    alcohol_consumed_2='100580-1.0',
    alcohol_consumed_3='100580-2.0',
    alcohol_consumed_4='100580-3.0',
    alcohol_consumed_5='100580-4.0',
    
    red_wine_1='100590-0.0',  #red wine                 code:100006
    red_wine_2='100590-1.0', 
    red_wine_3='100590-2.0', 
    red_wine_4='100590-3.0', 
    red_wine_5='100590-4.0', 
    
    rose_wine_1='100630-0.0',  #rose wine
    rose_wine_2='100630-1.0',
    rose_wine_3='100630-2.0',
    rose_wine_4='100630-3.0',
    rose_wine_5='100630-4.0',
    
    white_wine_1='100670-0.0',  #white wine
    white_wine_2='100670-1.0',
    white_wine_3='100670-2.0',
    white_wine_4='100670-3.0',
    white_wine_5='100670-4.0',
    
    beer_1='100710-0.0',  #beer/cider
    beer_2='100710-1.0',
    beer_3='100710-2.0',
    beer_4='100710-3.0',
    beer_5='100710-4.0',
    
    fortified_wine_1='100720-0.0',  #fortified wine
    fortified_wine_2='100720-1.0',
    fortified_wine_3='100720-2.0',
    fortified_wine_4='100720-3.0',
    fortified_wine_5='100720-4.0',
    
    spirits_1='100730-0.0',  #spirits
    spirits_2='100730-1.0',
    spirits_3='100730-2.0',
    spirits_4='100730-3.0',
    spirits_5='100730-4.0',
    
    other_alcohol_1='100740-0.0',  #other alcohol
    other_alcohol_2='100740-1.0',
    other_alcohol_3='100740-2.0',
    other_alcohol_4='100740-3.0',
    other_alcohol_5='100740-4.0',
    
    drink_status_1='20117-0.0',#Alcohol drinker status	饮酒状态"-3：Prefer not to answer   0：Never  1：Previous  2	Current"
    drink_status_2='20117-1.0',
    drink_status_3='20117-2.0',
    drink_status_4='20117-3.0',     
    
    
    ##----2.11 工作----
    c_employment_c="20119-0.0", #current employment status-corrected
    
    c_employment_00='6142-0.0',  #current employment status
    c_employment_01='6142-0.1',
    c_employment_02='6142-0.2',
    c_employment_03='6142-0.3',
    c_employment_04='6142-0.4',
    c_employment_05='6142-0.5',
    c_employment_06='6142-0.6',
    c_employment_10='6142-1.0',  
    c_employment_11='6142-1.1',
    c_employment_12='6142-1.2',
    c_employment_13='6142-1.3',
    c_employment_14='6142-1.4',
    c_employment_15='6142-1.5',
    c_employment_16='6142-1.6',
    c_employment_20='6142-2.0',  
    c_employment_21='6142-2.1',
    c_employment_22='6142-2.2',
    c_employment_23='6142-2.3',
    c_employment_24='6142-2.4',
    c_employment_25='6142-2.5',
    c_employment_26='6142-2.6',
    c_employment_30='6142-3.0', 
    c_employment_31='6142-3.1',
    c_employment_32='6142-3.2',
    c_employment_33='6142-3.3',
    c_employment_34='6142-3.4',
    c_employment_35='6142-3.5',
    c_employment_36='6142-3.6',
  
    
    ##----2.12 家庭收入----
    household_income_1='738-0.0',             #平均税前家庭总收入 code :100294
    household_income_2='738-1.0', 
    household_income_3='738-2.0', 
    household_income_4='738-3.0', 
    
    household_income_pilot='10877-0.0',#   平均税前家庭总收入(pilot) code :100657
    
    
    ##----2.13 实验室指标----
    apolp_a_1='30630-0.0',#载脂蛋白A
    apolp_a_2='30630-1.0',
    apolp_b_1='30640-0.0',#载脂蛋白B
    apolp_b_2='30640-1.0',
    lp_a_1='30790-0.0',#脂蛋白A
    lp_a_2='30790-1.0',
    cho_1='30690-0.0',#胆固醇
    cho_2='30690-1.0',
    tc_1='30870-0.0',#TC-甘油三酯
    tc_2='30870-1.0',
    hdl_1='30760-0.0',#HDL
    hdl_2='30760-1.0',
    ldl_1='30780-0.0',#LDL
    ldl_2='30780-1.0',
    
    glu_1='30740-0.0',#血糖
    glu_2='30740-1.0',#血糖
    
    HbA1c_1='30750-0.0',#糖化血红蛋白
    HbA1c_2='30750-1.0',#糖化血红蛋白
    
    creatinine='30700-0.0',#肌酐
    crp='30710-0.0',#C反应蛋白
    gamma='30730-0.0',#   γ-谷氨酰转移酶
    oestradiol='30800-0.0',#雌二醇
    phosphate='30810-0.0',#磷酸盐
    rheumatoidfactor='30820-0.0',#类风湿因子
    SHBG='30830-0.0',#SHBG-性激素结合球蛋白
    totalbilirubin='30840-0.0',#总胆红素
    testosterone='30850-0.0',#睾酮
    totalprotein='30860-0.0',#总蛋白
    urate='30880-0.0',#尿酸盐
    vitamin_D='30890-0.0',#Vitamin D
    

    ##----2.14 用药情况----
    medication_00='6153-0.0',#Medication for cholesterol, blood pressure, diabetes, or take exogenous hormones	治疗胆固醇、血压、糖尿病、或服用外源性激素的药物
    medication_01='6153-0.1',   #Females only
    medication_02='6153-0.2',
    medication_03='6153-0.3',
    medication_10='6153-1.0',
    medication_11='6153-1.1',
    medication_12='6153-1.2',
    medication_13='6153-1.3', 
    medication_20='6153-2.0',
    medication_21='6153-2.1',
    medication_22='6153-2.2',
    medication_23='6153-2.3', 
    medication_30='6153-3.0',
    medication_31='6153-3.1',
    medication_32='6153-3.2',
    medication_33='6153-3.3',     
    
    med_00='6177-0.0',#Medication for cholesterol, blood pressure or diabetes	治疗胆固醇、血压或糖尿病的药物
    med_01='6177-0.1',    #Males only
    med_02='6177-0.2',
    med_10='6177-1.0',    
    med_11='6177-1.1',
    med_12='6177-1.2',
    med_20='6177-2.0',   
    med_21='6177-2.1',
    med_22='6177-2.2',
    med_30='6177-3.0',    
    med_31='6177-3.1',
    med_32='6177-3.2',    
    
    treatment='20003-0.0',#Treatment/medication code	治疗、药物编码：4

    ##----2.15 糖尿病诊断----
    dm_diag_1='2443-0.0',#Diabetes diagnosed by doctor	糖尿病诊断
    dm_diag_2='2443-1.0',
    dm_diag_3='2443-2.0',
    dm_diag_4='2443-3.0',
    

    
    ##----2.16  自报的癌症----
    #--------------------medical conditions 疾病状态
    cancer_reported_00='20001-0.0',  #  cancer code, self reported
    cancer_reported_01='20001-0.1',
    cancer_reported_02='20001-0.2',
    cancer_reported_03='20001-0.3',
    cancer_reported_04='20001-0.4',
    cancer_reported_05='20001-0.5',
    cancer_reported_10='20001-1.0',  #  1
    cancer_reported_11='20001-1.1',
    cancer_reported_12='20001-1.2',
    cancer_reported_13='20001-1.3',
    cancer_reported_14='20001-1.4',
    cancer_reported_15='20001-1.5',
    cancer_reported_20='20001-2.0',  #  2
    cancer_reported_21='20001-2.1',
    cancer_reported_22='20001-2.2',
    cancer_reported_23='20001-2.3',
    cancer_reported_24='20001-2.4',
    cancer_reported_25='20001-2.5',
    cancer_reported_30='20001-3.0',  #  3
    cancer_reported_31='20001-3.1',
    cancer_reported_32='20001-3.2',
    cancer_reported_33='20001-3.3',
    cancer_reported_34='20001-3.4',
    cancer_reported_35='20001-3.5',
    
    num_cancer_1='134-0.0',  #number of self-reported cancers
    num_cancer_2='134-1.0',
    num_cancer_3='134-2.0',
    num_cancer_4='134-3.0',
    
    
    ##----2.17 自报的非癌疾病----
    #non-cancer illness code, self reported
    noncancer_00='20002-0.0',  noncancer_01='20002-0.1',  noncancer_02='20002-0.2',  noncancer_03='20002-0.3',  noncancer_04='20002-0.4',  noncancer_05='20002-0.5',  noncancer_06='20002-0.6',  noncancer_07='20002-0.7',  noncancer_08='20002-0.8',  noncancer_09='20002-0.9',
    noncancer_010='20002-0.10',noncancer_011='20002-0.11',noncancer_012='20002-0.12',noncancer_013='20002-0.13',noncancer_014='20002-0.14',noncancer_015='20002-0.15',noncancer_016='20002-0.16',noncancer_017='20002-0.17',noncancer_018='20002-0.18',noncancer_019='20002-0.19',
    noncancer_020='20002-0.20',noncancer_021='20002-0.21',noncancer_022='20002-0.22',noncancer_023='20002-0.23',noncancer_024='20002-0.24',noncancer_025='20002-0.25',noncancer_026='20002-0.26',noncancer_027='20002-0.27',noncancer_028='20002-0.28',noncancer_029='20002-0.29',
    noncancer_030='20002-0.30',noncancer_031='20002-0.31',noncancer_032='20002-0.32',noncancer_033='20002-0.33',
    
    noncancer_10='20002-1.0',  noncancer_11='20002-1.1',  noncancer_12='20002-1.2',  noncancer_13='20002-1.3',  noncancer_14='20002-1.4',  noncancer_15='20002-1.5',  noncancer_16='20002-1.6',  noncancer_17='20002-1.7',  noncancer_18='20002-1.8',  noncancer_19='20002-1.9',
    noncancer_110='20002-1.10',noncancer_111='20002-1.11',noncancer_112='20002-1.12',noncancer_113='20002-1.13',noncancer_114='20002-1.14',noncancer_115='20002-1.15',noncancer_116='20002-1.16',noncancer_117='20002-1.17',noncancer_118='20002-1.18',noncancer_119='20002-1.19',
    noncancer_120='20002-1.20',noncancer_121='20002-1.21',noncancer_122='20002-1.22',noncancer_123='20002-1.23',noncancer_124='20002-1.24',noncancer_125='20002-1.25',noncancer_126='20002-1.26',noncancer_127='20002-1.27',noncancer_128='20002-1.28',noncancer_129='20002-1.29',
    noncancer_130='20002-1.30',noncancer_131='20002-1.31',noncancer_132='20002-1.32',noncancer_133='20002-1.33',
    
    noncancer_20='20002-2.0',  noncancer_21='20002-2.1',  noncancer_22='20002-2.2',  noncancer_23='20002-2.3',  noncancer_24='20002-2.4',  noncancer_25='20002-2.5',  noncancer_26='20002-2.6',  noncancer_27='20002-2.7',  noncancer_28='20002-2.8',  noncancer_29='20002-2.9',
    noncancer_210='20002-2.10',noncancer_211='20002-2.11',noncancer_212='20002-2.12',noncancer_213='20002-2.13',noncancer_214='20002-2.14',noncancer_215='20002-2.15',noncancer_216='20002-2.16',noncancer_217='20002-2.17',noncancer_218='20002-2.18',noncancer_219='20002-2.19',
    noncancer_220='20002-2.20',noncancer_221='20002-2.21',noncancer_222='20002-2.22',noncancer_223='20002-2.23',noncancer_224='20002-2.24',noncancer_225='20002-2.25',noncancer_226='20002-2.26',noncancer_227='20002-2.27',noncancer_228='20002-2.28',noncancer_229='20002-2.29',
    noncancer_230='20002-2.30',noncancer_231='20002-2.31',noncancer_232='20002-2.32',noncancer_233='20002-2.33',
  
    noncancer_30='20002-3.0',  noncancer_31='20002-3.1',  noncancer_32='20002-3.2',  noncancer_33='20002-3.3',  noncancer_34='20002-3.4',  noncancer_35='20002-3.5',  noncancer_36='20002-3.6',  noncancer_37='20002-3.7',  noncancer_38='20002-3.8',  noncancer_39='20002-3.9',
    noncancer_310='20002-3.10',noncancer_311='20002-3.11',noncancer_312='20002-3.12',noncancer_313='20002-3.13',noncancer_314='20002-3.14',noncancer_315='20002-3.15',noncancer_316='20002-3.16',noncancer_317='20002-3.17',noncancer_318='20002-3.18',noncancer_319='20002-3.19',
    noncancer_320='20002-3.20',noncancer_321='20002-3.21',noncancer_322='20002-3.22',noncancer_323='20002-3.23',noncancer_324='20002-3.24',noncancer_325='20002-3.25',noncancer_326='20002-3.26',noncancer_327='20002-3.27',noncancer_328='20002-3.28',noncancer_329='20002-3.29',
    noncancer_330='20002-3.30',noncancer_331='20002-3.31',noncancer_332='20002-3.32',noncancer_333='20002-3.33',
    
    num_noncancer_1='135-0.0',  #number of self-reported non-cancer illnesses
    num_noncancer_2='135-1.0', 
    num_noncancer_3='135-2.0', 
    num_noncancer_4='135-3.0', 
    
    # cancer_year_00='84-0.0', #cancer year/age first occurred  
   
    
    ##----2.18 家族史----
    illness_father_00="20107-0.0",# Illnesses of  father  code:1010
    illness_father_01="20107-0.1",
    illness_father_02="20107-0.2",
    illness_father_03="20107-0.3",
    illness_father_04="20107-0.4",
    illness_father_05="20107-0.5",
    illness_father_06="20107-0.6",
    illness_father_07="20107-0.7",
    illness_father_08="20107-0.8",
    illness_father_09="20107-0.9",
    illness_father_10="20107-1.0",#1
    illness_father_11="20107-1.1",
    illness_father_12="20107-1.2",
    illness_father_13="20107-1.3",
    illness_father_14="20107-1.4",
    illness_father_15="20107-1.5",
    illness_father_16="20107-1.6",
    illness_father_17="20107-1.7",
    illness_father_18="20107-1.8",
    illness_father_19="20107-1.9",
    illness_father_20="20107-2.0",#2
    illness_father_21="20107-2.1",
    illness_father_22="20107-2.2",
    illness_father_23="20107-2.3",
    illness_father_24="20107-2.4",
    illness_father_25="20107-2.5",
    illness_father_26="20107-2.6",
    illness_father_27="20107-2.7",
    illness_father_28="20107-2.8",
    illness_father_29="20107-2.9",
    illness_father_30="20107-3.0",#3
    illness_father_31="20107-3.1",
    illness_father_32="20107-3.2",
    illness_father_33="20107-3.3",
    illness_father_34="20107-3.4",
    illness_father_35="20107-3.5",
    illness_father_36="20107-3.6",
    illness_father_37="20107-3.7",
    illness_father_38="20107-3.8",
    illness_father_39="20107-3.9",
    
    illness_mother_00="20110-0.0",# Illnesses of  mother
    illness_mother_01="20110-0.1",
    illness_mother_02="20110-0.2",
    illness_mother_03="20110-0.3",
    illness_mother_04="20110-0.4",
    illness_mother_05="20110-0.5",
    illness_mother_06="20110-0.6",
    illness_mother_07="20110-0.7",
    illness_mother_08="20110-0.8",
    illness_mother_09="20110-0.9",
    illness_mother_010="20110-0.10",
    illness_mother_10="20110-1.0",#1
    illness_mother_11="20110-1.1",
    illness_mother_12="20110-1.2",
    illness_mother_13="20110-1.3",
    illness_mother_14="20110-1.4",
    illness_mother_15="20110-1.5",
    illness_mother_16="20110-1.6",
    illness_mother_17="20110-1.7",
    illness_mother_18="20110-1.8",
    illness_mother_19="20110-1.9",
    illness_mother_110="20110-1.10",   
    illness_mother_20="20110-2.0",#2
    illness_mother_21="20110-2.1",
    illness_mother_22="20110-2.2",
    illness_mother_23="20110-2.3",
    illness_mother_24="20110-2.4",
    illness_mother_25="20110-2.5",
    illness_mother_26="20110-2.6",
    illness_mother_27="20110-2.7",
    illness_mother_28="20110-2.8",
    illness_mother_29="20110-2.9",
    illness_mother_210="20110-2.10", 
    illness_mother_30="20110-3.0",#3
    illness_mother_31="20110-3.1",
    illness_mother_32="20110-3.2",
    illness_mother_33="20110-3.3",
    illness_mother_34="20110-3.4",
    illness_mother_35="20110-3.5",
    illness_mother_36="20110-3.6",
    illness_mother_37="20110-3.7",
    illness_mother_38="20110-3.8",
    illness_mother_39="20110-3.9",
    illness_mother_310="20110-3.10", 
    
    illness_siblings_00="20111-0.0",# Illnesses of  siblings
    illness_siblings_01="20111-0.1",
    illness_siblings_02="20111-0.2",
    illness_siblings_03="20111-0.3",
    illness_siblings_04="20111-0.4",
    illness_siblings_05="20111-0.5",
    illness_siblings_06="20111-0.6",
    illness_siblings_07="20111-0.7",
    illness_siblings_08="20111-0.8",
    illness_siblings_09="20111-0.9",
    illness_siblings_010="20111-0.10",
    illness_siblings_011="20111-0.11",
    illness_siblings_10="20111-1.0",# 1
    illness_siblings_11="20111-1.1",
    illness_siblings_12="20111-1.2",
    illness_siblings_13="20111-1.3",
    illness_siblings_14="20111-1.4",
    illness_siblings_15="20111-1.5",
    illness_siblings_16="20111-1.6",
    illness_siblings_17="20111-1.7",
    illness_siblings_18="20111-1.8",
    illness_siblings_19="20111-1.9",
    illness_siblings_110="20111-1.10",
    illness_siblings_111="20111-1.11",
    illness_siblings_20="20111-2.0",# 2
    illness_siblings_21="20111-2.1",
    illness_siblings_22="20111-2.2",
    illness_siblings_23="20111-2.3",
    illness_siblings_24="20111-2.4",
    illness_siblings_25="20111-2.5",
    illness_siblings_26="20111-2.6",
    illness_siblings_27="20111-2.7",
    illness_siblings_28="20111-2.8",
    illness_siblings_29="20111-2.9",
    illness_siblings_210="20111-2.10",
    illness_siblings_211="20111-2.11",
    illness_siblings_30="20111-3.0",# 3
    illness_siblings_31="20111-3.1",
    illness_siblings_32="20111-3.2",
    illness_siblings_33="20111-3.3",
    illness_siblings_34="20111-3.4",
    illness_siblings_35="20111-3.5",
    illness_siblings_36="20111-3.6",
    illness_siblings_37="20111-3.7",
    illness_siblings_38="20111-3.8",
    illness_siblings_39="20111-3.9",
    illness_siblings_310="20111-3.10",
    illness_siblings_311="20111-3.11",
    
    

    ##----2.19 主要死亡结局----
    date_death_1='40000-0.0',#Date of death	死亡日期
    date_death_2='40000-1.0',
    
    primary_death_1='40001-0.0',#Underlying (primary) cause of death: ICD10	主要死亡原因 ICD-10  code:19
    primary_death_2='40001-1.0',  
    
    #-------次要死因--暂时不用
    # secondary_death_01='40002-0.1',#Contributory (secondary) causes of death: ICD10	次要死亡原因 ICD-10  code:19
    
    ##----2.20 发病结局----
    #直接从原始数据中读取  这里不再导入
    #次级诊断-----暂时不用？
    
    
    ##----2.2？ 其他（）----
    area='20118-0.0',#Home area population density - urban or rural	居住地人口密度，城市/农村

    family_n='709-0.0',#number in household 家庭成员数
    
    heart_diag='6150-0.0',#Vascular/heart problems diagnosed by doctor	医生诊断的心脏问题
    can_diag='2453-0.0',#Cancer diagnosed by doctor	  癌症诊断
    menopause='2724-0.0',#Had menopause	                绝经
    age_heart_atk='3894-0.0',#Age heart attack diagnosed	  心脏病发作的年龄
    age_pulmonary='4022-0.0',#Age pulmonary embolism (blood clot in lung) diagnosed	诊断肺栓塞的年龄
    age_stroke_diag= '4056-0.0',#Age stroke diagnosed        	诊断卒中的年龄

    date_lost='191-0.0',#Date lost to follow-up	失访日期

    date_mi='42000-0.0',#Date of myocardial infarction	MI日期
    date_stemi='42002-0.0',#Date of STEMI	STEMI日期
    date_nstemi='42004-0.0',#Date of NSTEMI	NSTEMI日期
    date_stroke='42006-0.0',#Date of stroke	Stroke日期
    date_istroke='42008-0.0',#Date of ischaemic stroke	I - stroke日期
    date_ih='42010-0.0',#Date of intracerebral haemorrhage	脑出血
    date_sh='42012-0.0',#Date of subarachnoid haemorrhage	蛛网膜下腔出血
    date_esrd='42026-0.0',#Date of end stage renal disease report	终末期肾病
    
    food_weight='100001-0.0',#Food weight	食物重量
    energy= '100002-0.0',#Energy	能量
    protein='100003-0.0',#Protein	蛋白质
    fat='100004-0.0',# Fat	脂肪
    carbohydrate='100005-0.0',#Carbohydrate	糖
    saturated_fat='100006-0.0',#Saturated fat	饱和脂肪
    polyunsaturated_fat	='100007-0.0',#Polyunsaturated fat	多不饱和脂肪
    total_sugars='100008-0.0',#Total sugars	总糖
    dietary_fibre='100009-0.0'# Englyst dietary fibre	膳食纤维
    
  )




#----3. 协变量构建----

##3.0  基线时间+随访终点时间----
ukb_apply <- ukb_apply %>% #计算基线时间
  mutate(date_bl=ifelse(is.na(date_bl_1)==TRUE,ifelse(is.na(date_bl_2)==TRUE,ifelse(is.na(date_bl_3)==TRUE,date_bl_4,date_bl_3),date_bl_2),date_bl_1)) %>% 
  mutate(date_bl=to_date(date_bl)) %>% 
  #给出统一的随访结束时间   时间是？？？
  mutate(date_end=as.Date("2021-04-22"))



##3.1 年龄 age ----
#    变量age_recruitment可用

ukb_apply$birth_day <- 15           #出生日均填补为15

ukb_apply <- ukb_apply %>% 
  mutate(date_birth=str_c(sep ="-",ukb_apply$birth_year,ukb_apply$birth_month,ukb_apply$birth_day))#构建出生日期变量

ukb_apply$date_birth <- ymd(ukb_apply$date_birth)#格式日期化

ukb_apply<- ukb_apply %>% 
  mutate(age=as.numeric(round(difftime(time1 = date_bl,time2 =date_birth ,units = "days")/365,2))) %>% #计算年龄，保留2位小数
  mutate(age_g=case_when(
    age<65~0,
    age>=65~1
  ))



##3.2 性别 sex ----
#    0-female 1-male


##3.3 种族 race ----
#   race  1、1001、1002、1003为White ethnicity or race，其余为other  还有NA值
ukb_apply <- ukb_apply %>% 
  mutate(race=ifelse(is.na(race_1)==TRUE,ifelse(is.na(race_2)==TRUE,race_3,race_2),race_1)) %>% 
  mutate(race_white=case_when(
    race =="1"|race =="1001"|race =='1002'|race =="1003"~"0",
    race =="2001"|race =="3001"|race =="4001"|
    race =="2"|race =="2002"|race =="3002"|race =="4002"|
    race =="3"|race =="2003"|race =="3003"|race =="4003"|
    race =="4"|race =="2004"|race =="3004"|race =="5"|race =="6"~"1"))



##3.4 自报的疾病----
#hyp、dm     cvd、cancer、emphysema，chronic bronchitis, or COPD

###3.4.1 自报的cancer✅
ukb_apply[["cancer_reported"]] <- apply(ukb_apply[,grep("^cancer_reported_", names(ukb_apply))]*0 == 0,
                                        1, any, na.rm = TRUE)*1
#     summary(factor(ukb_apply$cancer_reported))


###3.4.2 自报的hyp  ✅
ukb_apply[["hyp_reported"]] <- apply(  ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1065|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1072|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1073,
                                       1, any, na.rm = TRUE)*1
#     summary(factor(ukb_apply$hyp_reported))

###3.4.3 自报的dm  ✅
ukb_apply[["dm_reported"]] <- apply(   ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1220|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1221|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1222|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1223,
                                       1, any, na.rm = TRUE)*1
#     summary(factor(ukb_apply$dm_reported))

###3.4.4 自报的其他疾病  ✅
ukb_apply[["other_reported"]] <- apply( ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1113|
                                          ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1412|
                                          ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1472|
                                          ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1496|
                                          ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1112,
                                        1, any, na.rm = TRUE)*1
#     summary(factor(ukb_apply$other_reported))


###3.4.5 自报的CVD  ✅   除高血压（1065 1072 1073）之外
ukb_apply[["cvd_reported"]] <- apply(  ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1066|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1067|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1068|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1074|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1075|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1076|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1077|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1078|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1079|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1080|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1087|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1088|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1093|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1094|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1426|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1471|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1473|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1479|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1483|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1484|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1485|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1486|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1487|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1488|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1489|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1490|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1492|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1493|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1494|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1495|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1584|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1585|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1586|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1587|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1588|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1589|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1590|
                                         ukb_apply[,grep("^noncancer_", names(ukb_apply))] == 1593,
                                       1, any, na.rm = TRUE)*1
#     summary(factor(ukb_apply$cvd_reported))




##3.5   高血压病史  hyp----
#自报的hyp  SBP>140 |DBP>90 |服药

fun_mean <- function(x){
  mean(x,na.rm=TRUE)
}

ukb_apply <- ukb_apply %>% 
  mutate(sbp1=apply(ukb_apply[,c("sbp00","sbp01")],1,fun_mean),sbp2=apply(ukb_apply[,c("sbp10","sbp11")],1,fun_mean),#求sbp
         sbp3=apply(ukb_apply[,c("sbp20","sbp21")],1,fun_mean),sbp4=apply(ukb_apply[,c("sbp30","sbp31")],1,fun_mean),
         sbp=ifelse(is.na(sbp1)==TRUE,ifelse(is.na(sbp2)==TRUE,ifelse(is.na(sbp3)==TRUE,sbp4,sbp3),sbp2),sbp1)) %>% 
  mutate(dbp1=apply(ukb_apply[,c("dbp00","dbp01")],1,fun_mean),dbp2=apply(ukb_apply[,c("dbp10","dbp11")],1,fun_mean),#求dbp
         dbp3=apply(ukb_apply[,c("dbp20","dbp21")],1,fun_mean),dbp4=apply(ukb_apply[,c("dbp30","dbp31")],1,fun_mean),
         dbp=ifelse(is.na(dbp1)==TRUE,ifelse(is.na(dbp2)==TRUE,ifelse(is.na(dbp3)==TRUE,dbp4,dbp3),dbp2),dbp1))%>% 
  mutate(med_hyp=case_when(#判断服降压药
    med_00==2|med_01==2|med_02==2|
    med_10==2|med_11==2|med_12==2|
    med_20==2|med_21==2|med_22==2|
    med_30==2|med_31==2|med_32==2|
    medication_00==2|medication_01==2|medication_02==2|medication_03==2|
    medication_10==2|medication_11==2|medication_12==2|medication_13==2|
    medication_20==2|medication_21==2|medication_22==2|medication_23==2|
    medication_30==2|medication_31==2|medication_32==2|medication_33==2~"1",
    TRUE~"0"
  )) %>% 
  mutate(hyp=case_when(#判断是否高血压
    140>sbp & 90>dbp~0,
    sbp>=140|dbp>=90|med_hyp==1|hyp_reported==1~1
  ))

    
##3.6 糖尿病病史 dm----
ukb_apply <- ukb_apply %>% 
  mutate(dm_diag=ifelse(is.na(dm_diag_1)==TRUE,ifelse(is.na(dm_diag_2)==TRUE,ifelse(is.na(dm_diag_3)==TRUE,dm_diag_4,dm_diag_3),dm_diag_2),dm_diag_1)) %>%   #糖尿病诊断填补
  mutate(glu=ifelse(is.na(glu_1),glu_2,glu_1)) %>% #用血糖测量2填补测量1
  mutate(glu=ifelse(glu*18>500|glu*18<20,NA,glu)) %>% #剔除血糖>500或<20的异常值
  mutate(IFCC_HbA1c=ifelse(is.na(HbA1c_1),HbA1c_2,HbA1c_1)) %>% #用糖化血红蛋白测量2填补测量1  
  mutate(DCCT_HbA1c=(IFCC_HbA1c/10.929)+2.15) %>%   #单位：mmol/mol to %
  mutate(med_dm=case_when(#判断服降糖药
    med_00==3|med_01==3|med_02==3|
    med_10==3|med_11==3|med_12==3|
    med_20==3|med_21==3|med_22==3|
    med_30==3|med_31==3|med_32==3|
    medication_00==3|medication_01==3|medication_02==3|medication_03==3|
    medication_10==3|medication_11==3|medication_12==3|medication_13==3|
    medication_20==3|medication_21==3|medication_22==3|medication_23==3|
    medication_30==3|medication_31==3|medication_32==3|medication_33==3~"1",
    TRUE~"0"
    )) %>% 
  mutate(dm=case_when(#判断是否糖尿病  血糖正常值3.9-6.1 glu单位：mmol/L to mg/dl 要乘18
    126>glu*18 & glu*18>0~0,       #糖化血红蛋白 单位：mmol/mol
    glu*18>=126|DCCT_HbA1c>6.5 |med_dm==1|dm_diag==1|dm_reported==1~1
  ))

##3.7 血脂异常 dyslip----
ukb_apply <- ukb_apply %>% 
  mutate(cho=ifelse(is.na(cho_1),cho_2,cho_1)) %>% #用cho测量2填补测量1 单位：mmol/L to md/dl 乘38.67
  mutate(cho=ifelse(cho*38.67>500|cho*38.67<50,NA,cho)) %>% #剔除cho 异常值 >500|<50
  mutate(hdl=ifelse(is.na(hdl_1),hdl_2,hdl_1)) %>% #用hdl测量2填补测量1 单位：mmol/L to md/dl 乘38.67
  mutate(hdl=ifelse(hdl*38.67>200|hdl*38.67<10,NA,hdl)) %>% #剔除hdl 异常值 >200|<10
  mutate(ldl=ifelse(is.na(ldl_1),ldl_2,ldl_1)) %>% #用ldl测量2填补测量1 单位：mmol/L to md/dl 乘38.67
  mutate(ldl=ifelse(ldl*38.67>500|ldl*38.67<20,NA,ldl)) %>% #剔除ldl异常值 >500|<20
  mutate(tc=ifelse(is.na(tc_1),tc_2,tc_1)) %>% #用tc测量2填补测量1 单位：mmol/L to md/dl 乘88.57
  mutate(tc=ifelse(tc*88.57>1500|tc*88.57<15,NA,tc)) %>% #剔除tc 异常值 >1500|<15
  mutate(med_dyslip=case_when(#判断是否服降血脂药
    med_00==1|med_01==1|med_02==1|
    med_10==1|med_11==1|med_12==1|
    med_20==1|med_21==1|med_22==1|
    med_30==1|med_31==1|med_32==1|
    medication_00==1|medication_01==1|medication_02==1|medication_03==1|
    medication_10==1|medication_11==1|medication_12==1|medication_13==1|
    medication_20==1|medication_21==1|medication_22==1|medication_23==1|
    medication_30==1|medication_31==1|medication_32==1|medication_33==1~"1", 
    TRUE~"0"
    )) %>% 
  mutate(dyslip=case_when(     #判断是否血脂异常
    cho>0&hdl>0&ldl>0&tc>0|(is.na(med_dyslip)=FALSE)~0,
    cho>=6.2|hdl<1.04|ldl>=4.14|tc>=2.27|med_dyslip==1~1
  ))

##3.8 BMI  单位：kg/m2----
ukb_apply <- ukb_apply %>% 
  mutate(bmi=ifelse(is.na(bmi_1)==TRUE,ifelse(is.na(bmi_2)==TRUE,ifelse(is.na(bmi_3)==TRUE,bmi_4,bmi_3),bmi_2),bmi_1)) %>%    #以bmi多次测量数据填补前次
  mutate(bmi_g3=case_when(    #以25/30为界    <18.5的人非常少
    bmi<25~0,
    bmi>=25 & bmi<30~1,
    bmi>=30~2
  )) %>% 
  mutate(bmi_g2=case_when(    #以30为界
    bmi<30~0,
    bmi>=30~1
  ))

##3.9 smoke----
ukb_apply <- ukb_apply %>% 
  mutate(c_smoke=ifelse(is.na(c_smoke_1)==TRUE,ifelse(is.na(c_smoke_2)==TRUE,ifelse(is.na(c_smoke_3)==TRUE,c_smoke_4,c_smoke_3),c_smoke_2),c_smoke_1)) %>%    #以c_smoke多次测量数据填补前次
  mutate(p_smoke=ifelse(is.na(p_smoke_1)==TRUE,ifelse(is.na(p_smoke_2)==TRUE,ifelse(is.na(p_smoke_3)==TRUE,p_smoke_4,p_smoke_3),p_smoke_2),p_smoke_1)) %>%    #以p_smoke多次测量数据填补前次
  mutate(smoke100=ifelse(is.na(smoke100_1)==TRUE,ifelse(is.na(smoke100_2)==TRUE,ifelse(is.na(smoke100_3)==TRUE,smoke100_4,smoke100_3),smoke100_2),smoke100_1)) %>%    #以p_smoke多次测量数据填补前次
  mutate(smoke=case_when(   #c_smoke code:100347    p_smoke code:100348
    p_smoke==4|(p_smoke==2|p_smoke==3|c_smoke==2 & smoke100==2)~1,# 按照潘的计算  1:never smokeing  0:ever smokers
    c_smoke!=-3 | p_smoke!=-3 | !(smoke100==-1|smoke100==-3)~0
  ))

# ---UKB自带的吸烟状态  code:90
# mutate(smoke_status=ifelse(is.na(smoke_status_1)==TRUE,ifelse(is.na(smoke_status_2)==TRUE,ifelse(is.na(smoke_status_3)==TRUE,smoke_status_4,smoke_status_3),smoke_status_2),smoke_status_1)) %>%    #以smoke多次测量数据填补前次
#   mutate(smoke_status_g3=case_when(  #0：Never  1：Previous  2	Current
#     smoke_status==0~0,
#     smoke_status==1~1,
#     smoke_status==2~2
#   )) %>% 
#   mutate(smoke_status_g2=case_when(  #0：Never  1：Previous/Current
#     smoke_status==0~0,
#     smoke_status==1|smoke_status==2~1
#   ))


##3.10 drink   ----
#饮酒量？no heavy alcohol consumption?

ukb_apply <- ukb_apply %>% 
  mutate(alcohol_consumed=ifelse(is.na(alcohol_consumed_1)==TRUE,ifelse(is.na(alcohol_consumed_2)==TRUE,ifelse(is.na(alcohol_consumed_3)==TRUE,ifelse(is.na(alcohol_consumed_4)==TRUE,alcohol_consumed_5,alcohol_consumed_4),alcohol_consumed_3),alcohol_consumed_2),alcohol_consumed_1)) %>% #以多次测量填补前次
  mutate(red_wine=ifelse(is.na(red_wine_1)==TRUE,ifelse(is.na(red_wine_2)==TRUE,ifelse(is.na(red_wine_3)==TRUE,ifelse(is.na(red_wine_4)==TRUE,red_wine_5,red_wine_4),red_wine_3),red_wine_2),red_wine_1)) %>% #以多次测量填补前次
  mutate(num_red_wine=case_when(red_wine==555~0.5,red_wine==1~1,red_wine==2~2,red_wine==3~3,red_wine==4~4,red_wine==5~5,red_wine==600~6)) %>% # 按照code:100006计算量
  mutate(rose_wine=ifelse(is.na(rose_wine_1)==TRUE,ifelse(is.na(rose_wine_2)==TRUE,ifelse(is.na(rose_wine_3)==TRUE,ifelse(is.na(rose_wine_4)==TRUE,rose_wine_5,rose_wine_4),rose_wine_3),rose_wine_2),rose_wine_1)) %>% #以多次测量填补前次
  mutate(num_rose_wine=case_when(rose_wine==555~0.5,rose_wine==1~1,rose_wine==2~2,rose_wine==3~3,rose_wine==4~4,rose_wine==5~5,rose_wine==600~6)) %>% 
  mutate(white_wine=ifelse(is.na(white_wine_1)==TRUE,ifelse(is.na(white_wine_2)==TRUE,ifelse(is.na(white_wine_3)==TRUE,ifelse(is.na(white_wine_4)==TRUE,white_wine_5,white_wine_4),white_wine_3),white_wine_2),white_wine_1)) %>% #以多次测量填补前次
  mutate(num_white_wine=case_when(white_wine==555~0.5,white_wine==1~1,white_wine==2~2,white_wine==3~3,white_wine==4~4,white_wine==5~5,white_wine==600~6)) %>% 
  mutate(beer=ifelse(is.na(beer_1)==TRUE,ifelse(is.na(beer_2)==TRUE,ifelse(is.na(beer_3)==TRUE,ifelse(is.na(beer_4)==TRUE,beer_5,beer_4),beer_3),beer_2),beer_1)) %>% #以多次测量填补前次
  mutate(num_beer=case_when(beer==555~0.5,beer==1~1,beer==2~2,beer==3~3,beer==4~4,beer==5~5,beer==600~6)) %>% 
  mutate(fortified_wine=ifelse(is.na(fortified_wine_1)==TRUE,ifelse(is.na(fortified_wine_2)==TRUE,ifelse(is.na(fortified_wine_3)==TRUE,ifelse(is.na(fortified_wine_4)==TRUE,fortified_wine_5,fortified_wine_4),fortified_wine_3),fortified_wine_2),fortified_wine_1)) %>% #以多次测量填补前次
  mutate(num_fortified_wine=case_when(fortified_wine==555~0.5,fortified_wine==1~1,fortified_wine==2~2,fortified_wine==3~3,fortified_wine==4~4,fortified_wine==5~5,fortified_wine==600~6)) %>% 
  mutate(spirits=ifelse(is.na(spirits_1)==TRUE,ifelse(is.na(spirits_2)==TRUE,ifelse(is.na(spirits_3)==TRUE,ifelse(is.na(spirits_4)==TRUE,spirits_5,spirits_4),spirits_3),spirits_2),spirits_1)) %>% #以多次测量填补前次
  mutate(num_spirits=case_when(spirits==555~0.5,spirits==1~1,spirits==2~2,spirits==3~3,spirits==4~4,spirits==5~5,spirits==600~6)) %>% 
  mutate(other_alcohol=ifelse(is.na(other_alcohol_1)==TRUE,ifelse(is.na(other_alcohol_2)==TRUE,ifelse(is.na(other_alcohol_3)==TRUE,ifelse(is.na(other_alcohol_4)==TRUE,other_alcohol_5,other_alcohol_4),other_alcohol_3),other_alcohol_2),other_alcohol_1)) %>% #以多次测量填补前次
  mutate(num_other_alcohol=case_when(other_alcohol==555~0.5,other_alcohol==1~1,other_alcohol==2~2,other_alcohol==3~3,other_alcohol==4~4,other_alcohol==5~5,other_alcohol==600~6)) 
  

ukb_apply <- ukb_apply %>% 
  mutate(num_alcohol=rowSums(ukb_apply[,c("num_red_wine","num_rose_wine","num_white_wine","num_beer","num_fortified_wine","num_spirits","num_other_alcohol")],na.rm = TRUE)) %>% #计算饮酒总量
  mutate(drink_status=ifelse(is.na(drink_status_1)==TRUE,ifelse(is.na(drink_status_2)==TRUE,ifelse(is.na(drink_status_3)==TRUE,drink_status_4,drink_status_3),drink_status_2),drink_status_1)) %>%    #以多次测量数据填补前次
  mutate(drink=case_when(   #female<=1 male<=2  为健康饮酒
    (alcohol_consumed==0|freq_drink==0|drink_status==0)| #不饮酒
      (sex==0&(alcohol_consumed==1|freq_drink<=4&freq_drink>=1|drink_status==1|drink_status==2)&num_alcohol<=1)| #饮酒女性
      (sex==1&(alcohol_consumed==1|freq_drink<=4&freq_drink>=1|drink_status==1|drink_status==2)&num_alcohol<=2)~1, #饮酒男性
    TRUE~0
  ))
  
  
  # ---UKB自带的饮酒状态  code:90
  # mutate(drink_status=ifelse(is.na(drink_status_1)==TRUE,ifelse(is.na(drink_status_2)==TRUE,ifelse(is.na(drink_status_3)==TRUE,drink_status_4,drink_status_3),drink_status_2),drink_status_1)) %>%    #以smoke多次测量数据填补前次
  # mutate(drink_status_g3=case_when(  #0：Never  1：Previous  2	Current
  #   drink_status==0~0,
  #   drink_status==1~1,
  #   drink_status==2~2
  # )) %>% 
  # mutate(drink_status_g2=case_when(  #0：Never  1：Previous/Current
  #   drink_status==0~0,
  #   drink_status==1|drink_status==2~1
  # ))








##3.11  教育  edu ----
#以edu_0最优先，以edu_*更新填补以往的，以最高学历计算 12为college 3456为highschool  -7为无  -3为NA

ukb_apply <- ukb_apply %>% #  0:college 1:high school 2:less than highschool
  mutate(edu_p=case_when(
    edu_p0==1|edu_p1==1|edu_p2==1|edu_p3==1|edu_p4==1|
    edu_p0==2|edu_p1==2|edu_p2==2|edu_p3==2|edu_p4==2~0,
    edu_p0==3|edu_p1==3|edu_p2==3|edu_p3==3|edu_p4==3|
    edu_p0==4|edu_p1==4|edu_p2==4|edu_p3==4|edu_p4==4|
    edu_p0==5|edu_p1==5|edu_p2==5|edu_p3==5|edu_p4==5~1,
    edu_p0==-7|edu_p1==-7|edu_p2==-7|edu_p3==-7|edu_p4==-7~2
  )) %>% 
  mutate(edu_0=case_when(
    edu_00==1|edu_01==1|edu_02==1|edu_03==1|edu_04==1|edu_05==1|
    edu_00==2|edu_01==2|edu_02==2|edu_03==2|edu_04==2|edu_05==2~0,
    edu_00==3|edu_01==3|edu_02==3|edu_03==3|edu_04==3|edu_05==3|
    edu_00==4|edu_01==4|edu_02==4|edu_03==4|edu_04==4|edu_05==4|
    edu_00==5|edu_01==5|edu_02==5|edu_03==5|edu_04==5|edu_05==5|
    edu_00==6|edu_01==6|edu_02==6|edu_03==6|edu_04==6|edu_05==6~1,
    edu_00==-7|edu_01==-7|edu_02==-7|edu_03==-7|edu_04==-7|edu_05==-7~2
  )) %>% 
  mutate(edu_1=case_when(
    edu_10==1|edu_11==1|edu_12==1|edu_13==1|edu_14==1|edu_15==1|
    edu_10==2|edu_11==2|edu_12==2|edu_13==2|edu_14==2|edu_15==2~0,
    edu_10==3|edu_11==3|edu_12==3|edu_13==3|edu_14==3|edu_15==3|
    edu_10==4|edu_11==4|edu_12==4|edu_13==4|edu_14==4|edu_15==4|
    edu_10==5|edu_11==5|edu_12==5|edu_13==5|edu_14==5|edu_15==5|
    edu_10==6|edu_11==6|edu_12==6|edu_13==6|edu_14==6|edu_15==6~1,
    edu_10==-7|edu_11==-7|edu_12==-7|edu_13==-7|edu_14==-7|edu_15==-7~2
  )) %>% 
  mutate(edu_2=case_when(
    edu_20==1|edu_21==1|edu_22==1|edu_23==1|edu_24==1|edu_25==1|
    edu_20==2|edu_21==2|edu_22==2|edu_23==2|edu_24==2|edu_25==2~0,
    edu_20==3|edu_21==3|edu_22==3|edu_23==3|edu_24==3|edu_25==3|
    edu_20==4|edu_21==4|edu_22==4|edu_23==4|edu_24==4|edu_25==4|
    edu_20==5|edu_21==5|edu_22==5|edu_23==5|edu_24==5|edu_25==5|
    edu_20==6|edu_21==6|edu_22==6|edu_23==6|edu_24==6|edu_25==6~1,
    edu_20==-7|edu_21==-7|edu_22==-7|edu_23==-7|edu_24==-7|edu_25==-7~2
  )) %>% 
  mutate(edu_3=case_when(
    edu_30==1|edu_31==1|edu_32==1|edu_33==1|edu_34==1|edu_35==1|
    edu_30==2|edu_31==2|edu_32==2|edu_33==2|edu_34==2|edu_35==2~0,
    edu_30==3|edu_31==3|edu_32==3|edu_33==3|edu_34==3|edu_35==3|
    edu_30==4|edu_31==4|edu_32==4|edu_33==4|edu_34==4|edu_35==4|
    edu_30==5|edu_31==5|edu_32==5|edu_33==5|edu_34==5|edu_35==5|
    edu_30==6|edu_31==6|edu_32==6|edu_33==6|edu_34==6|edu_35==6~1,
    edu_30==-7|edu_31==-7|edu_32==-7|edu_33==-7|edu_34==-7|edu_35==-7~2
  )) %>% 
  mutate(edu=ifelse(is.na(edu_0)==TRUE,ifelse(is.na(edu_1)==TRUE,ifelse(is.na(edu_2)==TRUE,ifelse(is.na(edu_3)==TRUE,edu_p,edu_3),edu_2),edu_1),edu_0))


##3.12 体力活动----
ukb_apply <- ukb_apply %>% # IPAQ 0=low 1=moderate 2= high
  mutate(met_sum=met_walking+met_moderate+met_vigorous) %>% # 总活动时间
  mutate(physical_activity=case_when(# 1:理想  0:poor
    met_moderate>=150|met_vigorous>=75|met_moderate+met_vigorous>=150~1,
   # (met_moderate>0&met_moderate<150)|(met_vigorous>0&met_vigorous<75)|(met_moderate+met_vigorous>0&met_moderate+met_vigorous<150)~1,
    TRUE~0
  )) 
  
  
##3.13 睡眠时长  sleep_g----
ukb_apply <- ukb_apply %>% #以多次的数据填补第一次
  mutate(sleep_duration=ifelse(is.na(sleep_duration_1)==TRUE,ifelse(is.na(sleep_duration_2)==TRUE,ifelse(is.na(sleep_duration_3)==TRUE,sleep_duration_4,sleep_duration_3),sleep_duration_2),sleep_duration_1)) %>%
  mutate(sleep_duration=ifelse(sleep_duration==-3|sleep_duration==-1,NA,sleep_duration)) %>% #-1、-3 为NA
  mutate(sleep_g=case_when(  #<7或>8h 为0:poor   7-8h为1:good 
    sleep_duration>=7 & sleep_duration<=8~1,
    sleep_duration<7|sleep_duration>8~0
  ))


##3.14 饮食-----
     #fruit>=3   vegetable>=3  whole grains >=3  (shell)fish >=2
     #乳制品dairy>=2   vegetable oils >=2 refined grains<=2 processed meats <=1
     #unprocessed meats <=2  sugar-sweetened beverages 0
ukb_apply <- ukb_apply %>% #已多次测量数据填补最初
  mutate(fresh_fruit=ifelse(is.na(fresh_fruit_1)==TRUE,ifelse(is.na(fresh_fruit_2)==TRUE,ifelse(is.na(fresh_fruit_3)==TRUE,fresh_fruit_4,fresh_fruit_3),fresh_fruit_2),fresh_fruit_1)) %>%
  mutate(dried_fruit=ifelse(is.na(dried_fruit_1)==TRUE,ifelse(is.na(dried_fruit_2)==TRUE,ifelse(is.na(dried_fruit_3)==TRUE,dried_fruit_4,dried_fruit_3),dried_fruit_2),dried_fruit_1)) %>%
  mutate(cooked_vegetable=ifelse(is.na(cooked_vegetable_1)==TRUE,ifelse(is.na(cooked_vegetable_2)==TRUE,ifelse(is.na(cooked_vegetable_3)==TRUE,cooked_vegetable_4,cooked_vegetable_3),cooked_vegetable_2),cooked_vegetable_1)) %>%
  mutate(raw_vegetable=ifelse(is.na(raw_vegetable_1)==TRUE,ifelse(is.na(raw_vegetable_2)==TRUE,ifelse(is.na(raw_vegetable_3)==TRUE,raw_vegetable_4,raw_vegetable_3),raw_vegetable_2),raw_vegetable_1)) %>%
  mutate(bread_intake=ifelse(is.na(bread_intake_1)==TRUE,ifelse(is.na(bread_intake_2)==TRUE,ifelse(is.na(bread_intake_3)==TRUE,bread_intake_4,bread_intake_3),bread_intake_2),bread_intake_1)) %>%
  mutate(bread_type=ifelse(is.na(bread_type_1)==TRUE,ifelse(is.na(bread_type_2)==TRUE,ifelse(is.na(bread_type_3)==TRUE,bread_type_4,bread_type_3),bread_type_2),bread_type_1)) %>%
  mutate(cereal_intake=ifelse(is.na(cereal_intake_1)==TRUE,ifelse(is.na(cereal_intake_2)==TRUE,ifelse(is.na(cereal_intake_3)==TRUE,cereal_intake_4,cereal_intake_3),cereal_intake_2),cereal_intake_1)) %>%
  mutate(cereal_type=ifelse(is.na(cereal_type_1)==TRUE,ifelse(is.na(cereal_type_2)==TRUE,ifelse(is.na(cereal_type_3)==TRUE,cereal_type_4,cereal_type_3),cereal_type_2),cereal_type_1)) %>%
  mutate(oily_fish=ifelse(is.na(oily_fish_1)==TRUE,ifelse(is.na(oily_fish_2)==TRUE,ifelse(is.na(oily_fish_3)==TRUE,oily_fish_4,oily_fish_3),oily_fish_2),oily_fish_1)) %>%
  mutate(non_oily_fish=ifelse(is.na(non_oily_fish_1)==TRUE,ifelse(is.na(non_oily_fish_2)==TRUE,ifelse(is.na(non_oily_fish_3)==TRUE,non_oily_fish_4,non_oily_fish_3),non_oily_fish_2),non_oily_fish_1)) %>%
  mutate(cheese_intake=ifelse(is.na(cheese_intake_1)==TRUE,ifelse(is.na(cheese_intake_2)==TRUE,ifelse(is.na(cheese_intake_3)==TRUE,cheese_intake_4,cheese_intake_3),cheese_intake_2),cheese_intake_1)) %>%
  mutate(milk_type=ifelse(is.na(milk_type_1)==TRUE,ifelse(is.na(milk_type_2)==TRUE,ifelse(is.na(milk_type_3)==TRUE,milk_type_4,milk_type_3),milk_type_2),milk_type_1)) %>%
  mutate(processed_meat=ifelse(is.na(processed_meat_1)==TRUE,ifelse(is.na(processed_meat_2)==TRUE,ifelse(is.na(processed_meat_3)==TRUE,processed_meat_4,processed_meat_3),processed_meat_2),processed_meat_1)) %>%
  mutate(age_meat=ifelse(is.na(age_meat_1)==TRUE,ifelse(is.na(age_meat_2)==TRUE,ifelse(is.na(age_meat_3)==TRUE,age_meat_4,age_meat_3),age_meat_2),age_meat_1)) %>%
  mutate(poultry=ifelse(is.na(poultry_1)==TRUE,ifelse(is.na(poultry_2)==TRUE,ifelse(is.na(poultry_3)==TRUE,poultry_4,poultry_3),poultry_2),poultry_1)) %>%
  mutate(beef=ifelse(is.na(beef_1)==TRUE,ifelse(is.na(beef_2)==TRUE,ifelse(is.na(beef_3)==TRUE,beef_4,beef_3),beef_2),beef_1)) %>%
  mutate(lamb=ifelse(is.na(lamb_1)==TRUE,ifelse(is.na(lamb_2)==TRUE,ifelse(is.na(lamb_3)==TRUE,lamb_4,lamb_3),lamb_2),lamb_1)) %>%
  mutate(pork=ifelse(is.na(pork_1)==TRUE,ifelse(is.na(pork_2)==TRUE,ifelse(is.na(pork_3)==TRUE,pork_4,pork_3),pork_2),pork_1)) %>%
  mutate(ssb=ifelse(is.na(ssb_1)==TRUE,ifelse(is.na(ssb_2)==TRUE,ifelse(is.na(ssb_3)==TRUE,ssb_4,ssb_3),ssb_2),ssb_1)) %>%
  mutate(non_butter=ifelse(is.na(non_butter_1)==TRUE,ifelse(is.na(non_butter_2)==TRUE,ifelse(is.na(non_butter_3)==TRUE,non_butter_4,non_butter_3),non_butter_2),fresh_fruit_1)) %>%
  mutate(spread_type=ifelse(is.na(spread_type_1)==TRUE,ifelse(is.na(spread_type_2)==TRUE,ifelse(is.na(spread_type_3)==TRUE,spread_type_4,spread_type_3),spread_type_2),fresh_fruit_1)) %>%
  #处理  按照各自code   
  mutate(fresh_fruit=ifelse(fresh_fruit==-1|fresh_fruit==-3,NA,ifelse(fresh_fruit==-10,0.5,fresh_fruit))) %>%       #code:10373  不知道-1/不回答-3 赋NA      -10 小于一 赋0.5 小于1就行不影响最后判别
  mutate(dried_fruit=ifelse(dried_fruit==-1|dried_fruit==-3,NA,ifelse(dried_fruit==-10,0.5,dried_fruit))) %>% 
  mutate(cooked_vegetable=ifelse(cooked_vegetable==-1|cooked_vegetable==-3,NA,ifelse(cooked_vegetable==-10,0.5,cooked_vegetable))) %>% 
  mutate(raw_vegetable=ifelse(raw_vegetable==-1|raw_vegetable==-3,NA,ifelse(raw_vegetable==-10,0.5,raw_vegetable))) %>% 
  mutate(bread_intake=ifelse(bread_intake==-1|bread_intake==-3,NA,ifelse(bread_intake==-10,0.5,bread_intake))) %>% 
  mutate(cereal_intake=ifelse(cereal_intake==-1|cereal_intake==-3,NA,ifelse(cereal_intake==-10,0.5,cereal_intake))) %>% 
  mutate(age_meat=ifelse(age_meat==-1|cereal_intake==-3,NA,age_meat)) # code 100291
  
ukb_apply <- ukb_apply %>%   #构造膳食变量
  mutate(fruit=case_when(  #水果     鲜+干/5 >=3
    fresh_fruit+dried_fruit/5>=3~1,
    TRUE~0
  )) %>% 
  mutate(vegetable=case_when(  #(鲜+干)/3 >=3
    (cooked_vegetable+raw_vegetable)/3>=3~1,
    TRUE~0
  )) %>% 
  mutate(whole_grains=case_when(   #全谷物  (面包:3+燕麦：134)  /7>=3
    (bread_type==3 &  !(cereal_type==1|cereal_type==3|cereal_type==4) & bread_intake/7>=3)|  #只吃面包
    (bread_type!=3 &   (cereal_type==1|cereal_type==3|cereal_type==4) & cereal_intake/7>=3)|  #只吃燕麦
    (bread_type==3 &   (cereal_type==1|cereal_type==3|cereal_type==4) & (bread_intake+cereal_type)/7>=3) ~1,  #都吃
    TRUE~0
  )) %>% 
  mutate(fish=case_when(   #鱼类  油性+非油性>=2
    oily_fish>2|non_oily_fish>2|(oily_fish==2 & non_oily_fish==2)~1,
    TRUE~0
  ))  %>% 
  mutate(dairy=case_when(   #奶制品 奶酪6 +牛奶1-5  则==    >=2
    cheese_intake==6 & (milk_type==1|milk_type==2|milk_type==3|milk_type==4|milk_type==5)~1,
    TRUE~0
  )) %>% 
  mutate(vegetable_oils=case_when(   #植物油  spread:2/non_butter:24678 且2片面包为1 serving  除以7 >=4
    spread_type==2|non_butter==2|non_butter==4|non_butter==6|non_butter==7|non_butter==8 & bread_intake/7 >=4~1,
    TRUE~0
  )) %>% 
  mutate(refined_grains=case_when(    #精制谷物  面包:124+燕麦：25 <=2
    ( (bread_type==1|bread_type==2|bread_type==4) & !(cereal_type==2|cereal_type==5) & bread_intake/7<=2)|  #只吃面包
    (!(bread_type==1|bread_type==2|bread_type==4) &  (cereal_type==2|cereal_type==5) & bread_intake/7<=2)| #只吃燕麦
    ( (bread_type==1|bread_type==2|bread_type==4) &  (cereal_type==2|cereal_type==5) & (bread_intake+cereal_type)/7<=2) ~1,   #都吃
  TRUE~0
  )) %>% 
  mutate(p_meat=case_when(       #加工肉
    processed_meat==0|processed_meat==1|processed_meat==2| age-age_meat>=10~1,
    TRUE~0
  )) %>% 
  mutate(    #未加工肉
    poultry_n=case_when(poultry==0~0,poultry==1~0.5,poultry==2~1),
    beef_n=case_when(beef==0~0,beef==1~0.5,beef==2~1),
    lamb_n=case_when(lamb==0~0,lamb==1~0.5,lamb==2~1),
    pork_n=case_when(pork==0~0,pork==1~0.5,pork==2~1),
    unp_meats=case_when(  (poultry_n+beef_n+lamb_n+pork_n<=2) | age-age_meat>=10~1,TRUE~0)
  ) %>% 
  mutate(sugar_sb=case_when(  #含糖饮料 
    ssb==1|ssb==2|ssb==3~1,
    TRUE~0
  )) %>% 
  mutate(diet_score=fruit+vegetable+whole_grains+fish+dairy+vegetable_oils+refined_grains+p_meat+unp_meats+sugar_sb) %>%  # 健康膳食得分
  mutate(diet_score_g=case_when(   #健康膳食得分>=5 为1
    diet_score>=5~1,
    TRUE~0
  ))


##3.15 家庭收入----
ukb_apply <- ukb_apply %>% 
  mutate(household_income=ifelse(is.na(household_income_1)==TRUE,ifelse(is.na(household_income_2)==TRUE,ifelse(is.na(household_income_3)==TRUE,household_income_4,household_income_3),household_income_2),household_income_1)) %>% #多次数据填补
  mutate(household_income=ifelse(is.na(household_income)==TRUE,household_income_pilot,household_income)) %>% #pilot数据 填补
  mutate(income_level=case_when(    #code :100294   0:low 1:medium 2:high
    household_income==1~0,
    household_income==2|household_income==3~1,
    household_income==4|household_income==5~2
  ))


##3.16  职业情况----
ukb_apply <- ukb_apply %>%  #同一批回答 以多次数据填补
  mutate(c_employment_0=ifelse(is.na(c_employment_00)==TRUE,ifelse(is.na(c_employment_01)==TRUE,ifelse(is.na(c_employment_02)==TRUE,ifelse(is.na(c_employment_03)==TRUE,ifelse(is.na(c_employment_04)==TRUE,ifelse(is.na(c_employment_05)==TRUE,c_employment_06,c_employment_05),c_employment_04),c_employment_03),c_employment_02),c_employment_01),c_employment_00)) %>% 
  mutate(c_employment_1=ifelse(is.na(c_employment_10)==TRUE,ifelse(is.na(c_employment_11)==TRUE,ifelse(is.na(c_employment_12)==TRUE,ifelse(is.na(c_employment_13)==TRUE,ifelse(is.na(c_employment_14)==TRUE,ifelse(is.na(c_employment_15)==TRUE,c_employment_16,c_employment_15),c_employment_14),c_employment_13),c_employment_12),c_employment_11),c_employment_10)) %>% 
  mutate(c_employment_2=ifelse(is.na(c_employment_20)==TRUE,ifelse(is.na(c_employment_21)==TRUE,ifelse(is.na(c_employment_22)==TRUE,ifelse(is.na(c_employment_23)==TRUE,ifelse(is.na(c_employment_24)==TRUE,ifelse(is.na(c_employment_25)==TRUE,c_employment_26,c_employment_25),c_employment_24),c_employment_23),c_employment_22),c_employment_21),c_employment_20)) %>% 
  mutate(c_employment_3=ifelse(is.na(c_employment_30)==TRUE,ifelse(is.na(c_employment_31)==TRUE,ifelse(is.na(c_employment_32)==TRUE,ifelse(is.na(c_employment_33)==TRUE,ifelse(is.na(c_employment_34)==TRUE,ifelse(is.na(c_employment_35)==TRUE,c_employment_36,c_employment_35),c_employment_34),c_employment_33),c_employment_32),c_employment_31),c_employment_30)) %>% 
  #多批数据，以最新填补前次  
  mutate(employment=ifelse(is.na(c_employment_3)==TRUE,ifelse(is.na(c_employment_2)==TRUE,ifelse(is.na(c_employment_1)==TRUE,c_employment_0,c_employment_1),c_employment_2),c_employment_3)) %>% 
  #以校正的更新
  mutate(employment=ifelse(is.na(c_employment_c)==TRUE,employment,c_employment_c)) %>% 
  # 判断工作情况  1267为employed   345为unemployed  
  mutate(employment_status=case_when(  
    employment==1|employment==2|employment==6|employment==7~1,
    employment==3|employment==4|employment==5~0
  ))

##3.17 家族史----

###3.17.1 cvd家族史   #依次为心脏病1、中风2
ukb_apply[["cvd_father"]] <- apply(ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 1|
                                       ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 2,
                                     1, any, na.rm = TRUE)*1
ukb_apply[["cvd_mother"]] <- apply(ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 1|
                                     ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 2,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["cvd_siblings"]] <- apply(ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 1|
                                     ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 2,
                                   1, any, na.rm = TRUE)*1

###3.17.2  cancer家族史       #依次为肺癌3、肠癌4、乳腺癌5、前列腺癌13
ukb_apply[["cancer_father"]] <- apply(ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 3|
                                     ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 4|
                                     ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 5|
                                     ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 13,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["cancer_mother"]] <- apply(ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 3|
                                     ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 4|
                                     ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 5|
                                     ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 13,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["cancer_siblings"]] <- apply(ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 3|
                                       ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 4|
                                       ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 5|
                                       ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] == 13,
                                     1, any, na.rm = TRUE)*1

###3.17.3  hyp家族史   #高血压8
ukb_apply[["hyp_father"]] <- apply(ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 8,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["hyp_mother"]] <- apply(ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 8,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["hyp_siblings"]] <- apply(ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] ==8,
                                     1, any, na.rm = TRUE)*1

###3.17.4  dm家族史   #糖尿病9
ukb_apply[["dm_father"]] <- apply(ukb_apply[,grep("^illness_father_", names(ukb_apply))] == 9,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["dm_mother"]] <- apply(ukb_apply[,grep("^illness_mother_", names(ukb_apply))] == 9,
                                   1, any, na.rm = TRUE)*1
ukb_apply[["dm_siblings"]] <- apply(ukb_apply[,grep("^illness_siblings_", names(ukb_apply))] ==9,
                                     1, any, na.rm = TRUE)*1

# 构建家族史变量
ukb_apply <- ukb_apply %>%
  mutate(fh_cvd = case_when(cvd_father == 1 |
                              cvd_mother == 1 | cvd_siblings == 1 ~ 1,
                            TRUE ~ 0)) %>%
  mutate(fh_cancer = case_when(
    cancer_father == 1 | cancer_mother == 1 | cancer_siblings == 1 ~ 1,
    TRUE ~ 0
  )) %>%
  mutate(fh_hyp = case_when(hyp_father == 1 |
                              hyp_mother == 1 | hyp_siblings == 1 ~ 1,
                            TRUE ~ 0)) %>%
  mutate(fh_dm = case_when(dm_father == 1 |
                             dm_mother == 1 | dm_siblings == 1 ~ 1,
                           TRUE ~ 0))

#查看
summary(factor(ukb_apply$fh_cvd))





















#-------------   二、结局变量
##  cvd 发病/死亡？
##  cancer
##  all-cause


#4. ----死亡结局----
ukb_apply <- ukb_apply %>% #得到死亡时间\  主要死因ICD-10  ✅         
  mutate(date_death=as_date(ifelse(is.na(date_death_1)==TRUE,date_death_2,date_death_1),origin=lubridate::origin))%>% #以第二次填补，并转化为日期格式
  mutate(primary_death=ifelse(is.na(primary_death_1)==TRUE,primary_death_2,primary_death_1)) %>% #以第二次填补
  mutate(primary_death_3=str_sub(primary_death,1,3)) %>%  #截取死亡ICD-10前三位，如“I201”中的I20
  mutate(primary_death_4=str_sub(primary_death,4,4))      #截取死亡ICD-10第四位，如“I201”中的1


ukb_apply <- ukb_apply %>%
  ##4.0 all-cause mortality
  mutate(death=ifelse(is.na(primary_death)==TRUE,0,1)) %>% 
  ##4.1 CHD死亡
  mutate(death_chd=case_when(  
    primary_death_3=="I20"|primary_death_3=="I21"|primary_death_3=="I22"|primary_death_3=="I23"|
    primary_death_3=="I24"|primary_death_3=="I25"|(primary_death_3=="I46" & primary_death_4!="1")~1,
   TRUE~0
  )) %>% 
  ##4.2 stroke死亡
  mutate(death_stroke=case_when(
    primary_death_3=="I60"|primary_death_3=="I61"|primary_death_3=="I63"|primary_death_3=="I64"~1,
    TRUE~0
  )) %>% 
  ###4.2.1 缺血性
  mutate(death_stroke_i=case_when(
    primary_death_3=="I63"|primary_death_3=="I64"~1,
    TRUE~0
  )) %>% 
  ###4.2.2 出血性
  mutate(death_stroke_h=case_when(
    primary_death_3=="I60"|primary_death_3=="I61"~1,
    TRUE~0
  )) %>% 
  ##4.3 hf死亡
  mutate(death_hf=case_when(
    (primary_death_3=="I50" & primary_death_4!="8")|(primary_death_3=="I11" & primary_death_4=="0")|
    (primary_death_3=="I13" & primary_death_4=="0")|(primary_death_3=="I13" & primary_death_4=="2")~1,
    TRUE~0
  )) %>% 
  ##4.4 tia死亡9
  mutate(death_tia=case_when(
    primary_death_3=="G45" & primary_death_4!="4"~1,
    TRUE~0
  )) %>% 
  ##4.5 pvd死亡
  mutate(death_pvd=case_when(
    primary_death_3=="I65"|primary_death_3=="I70"|primary_death_3=="I71"|
    (primary_death_3=="I73" & primary_death_4=="9")|primary_death_3=="I74"~1,
    TRUE~0
  )) %>% 
  ##4.6 othercvd死亡
  mutate(death_othercvd=case_when(
    primary_death_3=="I66"|primary_death_3=="I67"|
    (primary_death_3=="I67" & primary_death_4=="2")|(primary_death_3=="I69" & primary_death_4!="2")~1,
    TRUE~0
  )) %>% 
  ##4.7  CVD死亡
  mutate(death_cvd=case_when(
    death_chd==1|death_stroke==1|death_hf==1|death_tia==1|death_pvd==1|death_othercvd==1~1,
    TRUE~0
  )) 

#查看       summary(factor(ukb_apply$death_cvd))
    







#5. ----发病结局----

#处理ICD的function
#各变量依次为：输出的发病变量名、输出的发病时间名、导入的变量名1和2、单一变量个数，ICD编码
fun_inc <- function(inc,date,start_1,start_2,n,ICD_code){
  #选择数据
  data_inc <- ukb_test %>% 
    select(starts_with(start_1),starts_with(start_2),eid)
  #建立空变量
  data_inc$inc <- 0
  data_inc$date <- NA
  #循环
  for (r in 1:nrow(data_inc)) {
    for (c in 1:n) {
      if (grepl(ICD_code,data_inc[r,c])==TRUE) {
        data_inc[r,c] <- 1
        data_inc$inc[r] <- 1
        data_inc$date[r] <- min(data_inc$date[r],data_inc[r,c+n],na.rm = TRUE)
      } else{data_inc[r,c] <- 0}
    }
  }
  #整理保留
  data_inc <- data_inc %>% 
    select(eid,inc,date) %>% 
    mutate(date=as_date(date,origin=lubridate::origin))
  names(data_inc) <- c("eid",inc,date)
  
  return(data_inc)
}

  #function测试
chd_inc_test <- fun_inc("inc_chd","date_chd","41270","41280",223,"^I20|^I21|^I22|^I23|^I24|^I25|^I460|^I469")

to_date <- function(x){
  as_date(x,origin=lubridate::origin)
}

##5.1 ----判断cvd发病---- 
# （一）# Diagnoses - ICD10       "41270","41280"
# （二）# Diagnoses - main ICD10  "41202","41262"
# （三）# Diagnoses - ICD9        "41271","41281"
# （四）# Diagnoses - main ICD9   "41203","41263"


####5.1.1（一）#Diagnoses - ICD10  "41270","41280" ----
#(1 CHD发病 ICD-10:  I20-I25, I46(除I461)
inc_chd <- fun_inc("inc_chd_10","date_chd_10","41270","41280",223,"^I20|^I21|^I22|^I23|^I24|^I25|^I460|^I469")

#(2 卒中发病  I60,I61,I63,I64
inc_stroke <- fun_inc("inc_stroke_10","date_stroke_10","41270","41280",223,"^I60|^I61|^I63|^I64")
inc_stroke_i <- fun_inc("inc_stroke_i_10","date_stroke_i_10","41270","41280",223,"^I63|^I64") # ischemic缺血性
inc_stroke_h <- fun_inc("inc_stroke_h_10","date_stroke_h_10","41270","41280",223,"^I60|^I61") # hemorrhagic出血性

#(3 心衰发病  I50(除I508),I110,I130,I132
inc_hf <- fun_inc("inc_hf_10","date_hf_10","41270","41280",223,"^I50|^I110|^I130|^I132")

#(4 TIA发病  G45(除G454)
inc_tia <- fun_inc("inc_tia_10","date_tia_10","41270","41280",223,"^G450|^G451|^G452|^G453|^G458|^G459")

#(5 PVD发病  I65,I702,I710,I711,I713,I715,I718,I739,I74
inc_pvd <- fun_inc("inc_pvd_10","date_pvd_10","41270","41280",223,"^I65|^I702|^I710|^I711|^I713|^I715|^I718|^I739|^I74")

#(6 othercvd发病  I66，I670
inc_othercvd <- fun_inc("inc_othercvd_10","date_othercvd_10","41270","41280",223,"^I66|^I670")

#合并
inc_cvd <- full_join(inc_chd,inc_stroke,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_i,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_h,by="eid")
inc_cvd <- full_join(inc_cvd,inc_hf,by="eid")
inc_cvd <- full_join(inc_cvd,inc_tia,by="eid")
inc_cvd <- full_join(inc_cvd,inc_pvd,by="eid")
inc_cvd <- full_join(inc_cvd,inc_othercvd,by="eid")

#对inc_cvd数据集进行处理
inc_cvd_10 <- inc_cvd %>% 
  rowwise() %>% 
  mutate(inc_cvd_10=case_when(
    inc_chd_10==1|inc_stroke_10==1|inc_hf_10==1|inc_tia_10==1|inc_pvd_10==1|inc_othercvd_10==1~1,
    TRUE~0
  )) %>% 
  mutate(date_cvd_10=ifelse(inc_cvd_10==1,min(c(date_chd_10,date_stroke_10,date_hf_10,date_tia_10,date_pvd_10,date_othercvd_10),na.rm = TRUE),NA
  )) %>% 
  mutate(date_cvd_10=to_date(date_cvd_10)) %>% 
  select(eid,inc_cvd_10,date_cvd_10,inc_chd_10,date_chd_10,inc_stroke_10,date_stroke_10,inc_stroke_i_10,date_stroke_i_10,inc_stroke_h_10,date_stroke_h_10)

#移除合并用到的数据集
rm(inc_chd,inc_stroke,inc_stroke_i,inc_stroke_h,inc_hf,inc_tia,inc_pvd,inc_othercvd,inc_cvd)




###5.1.2（二）#Diagnoses - main ICD10  "41202","41262"  ----

#(1 CHD发病 ICD-10:  I20-I25, I46(除I461)
inc_chd <- fun_inc("inc_chd_10_main","date_chd_10_main","41202","41262",75,"^I20|^I21|^I22|^I23|^I24|^I25|^I460|^I469")

#(2 卒中发病  I60,I61,I63,I64
inc_stroke <- fun_inc("inc_stroke_10_main","date_stroke_10_main","41202","41262",75,"^I60|^I61|^I63|^I64")
inc_stroke_i <- fun_inc("inc_stroke_i_10_main","date_stroke_i_10_main","41202","41262",75,"^I63|^I64") # ischemic缺血性
inc_stroke_h <- fun_inc("inc_stroke_h_10_main","date_stroke_h_10_main","41202","41262",75,"^I60|^I61") # hemorrhagic出血性

#(3 心衰发病  I50(除I508),I110,I130,I132
inc_hf <- fun_inc("inc_hf_10_main","date_hf_10_main","41202","41262",75,"^I50|^I110|^I130|^I132")

#(4 TIA发病  G45(除G454)
inc_tia <- fun_inc("inc_tia_10_main","date_tia_10_main","41202","41262",75,"^G450|^G451|^G452|^G453|^G458|^G459")

#(5 PVD发病  I65,I702,I710,I711,I713,I715,I718,I739,I74
inc_pvd <- fun_inc("inc_pvd_10_main","date_pvd_10_main","41202","41262",75,"^I65|^I702|^I710|^I711|^I713|^I715|^I718|^I739|^I74")

#(6 othercvd发病  I66，I670
inc_othercvd <- fun_inc("inc_othercvd_10_main","date_othercvd_10_main","41202","41262",75,"^I66|^I670")

#合并
inc_cvd <- full_join(inc_chd,inc_stroke,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_i,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_h,by="eid")
inc_cvd <- full_join(inc_cvd,inc_hf,by="eid")
inc_cvd <- full_join(inc_cvd,inc_tia,by="eid")
inc_cvd <- full_join(inc_cvd,inc_pvd,by="eid")
inc_cvd <- full_join(inc_cvd,inc_othercvd,by="eid")


#对inc_cvd_main数据集进行处理
inc_cvd_10_main <- inc_cvd %>% 
  rowwise() %>% 
  mutate(inc_cvd_10_main=case_when(
    inc_chd_10_main==1|inc_stroke_10_main==1|inc_hf_10_main==1|inc_tia_10_main==1|inc_pvd_10_main==1|inc_othercvd_10_main==1~1,
    TRUE~0
  )) %>% 
  mutate(date_cvd_10_main=ifelse(inc_cvd_10_main==1,min(c(date_chd_10_main,date_stroke_10_main,date_hf_10_main,date_tia_10_main,date_pvd_10_main,date_othercvd_10_main),na.rm = TRUE),NA
  )) %>% 
  mutate(date_cvd_10_main=to_date(date_cvd_10_main)) %>% 
  select(eid,inc_cvd_10_main,date_cvd_10_main,inc_chd_10_main,date_chd_10_main,inc_stroke_10_main,date_stroke_10_main,inc_stroke_i_10_main,date_stroke_i_10_main,inc_stroke_h_10_main,date_stroke_h_10_main)

#移除合并用到的数据集
rm(inc_chd,inc_stroke,inc_stroke_i,inc_stroke_h,inc_hf,inc_tia,inc_pvd,inc_othercvd,inc_cvd)




###5.1.3（三）# Diagnoses - ICD9  "41271" "41281"  ----

#(1 CHD发病 ICD-9:  410-414,4257
inc_chd <- fun_inc("inc_chd_9","date_chd_9","41271","41281",47,"^410|^411|^412|^413|^414|^4257")

#(2 卒中发病 430,431,(4330,,4331,4332,4338,4339,4340,4341)
inc_stroke <- fun_inc("inc_stroke_9","date_stroke_9","41271","41281",47,"^430|^431|^4330|^4331|^4332|^4338|^4339|^4340|^4341")
inc_stroke_i <- fun_inc("inc_stroke_i_9","date_stroke_i_9","41271","41281",47,"^4330|^4331|^4332|^4338|^4339|^4340|^4341") # ischemic缺血性
inc_stroke_h <- fun_inc("inc_stroke_h_9","date_stroke_h_9","41271","41281",47,"^430|^431") # hemorrhagic出血性

#(3 心衰发病  428,4020,4021,4029,4040,4041,4049
inc_hf <- fun_inc("inc_hf_9","date_hf_9","41271","41281",47,"^428|^4020|^4021|^4029|^4040|^4041|^4049")

#(4 TIA发病  4350,4351,4353,4358,3623,4352,4358,4359
inc_tia <- fun_inc("inc_tia_9","date_tia_9","41271","41281",47,"^4350|^4351|^4353|^4358|^3623|^4352|^4358|^4359")

#(5 PVD发病  4330,4331,4332,4338,4339,4402,7071,7079,4411,4413,4416,4415,4439,4440,4441,4442,4448,4449
inc_pvd <- fun_inc("inc_pvd_9","date_pvd_9","41271","41281",47,"^4330|^4331|^4332|^4338|^4339|^4402|^7071|^7079|^4411|^4413|^4416|^4415|^4439|^4440|^4441|^4442|^4448|^4449")

#(6 othercvd发病  4340,4341,4349,4432
inc_othercvd <- fun_inc("inc_othercvd_9","date_othercvd_9","41271","41281",47,"^4340|^4341|^4349|^4432")

#合并
inc_cvd <- full_join(inc_chd,inc_stroke,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_i,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_h,by="eid")
inc_cvd <- full_join(inc_cvd,inc_hf,by="eid")
inc_cvd <- full_join(inc_cvd,inc_tia,by="eid")
inc_cvd <- full_join(inc_cvd,inc_pvd,by="eid")
inc_cvd <- full_join(inc_cvd,inc_othercvd,by="eid")

#对inc_cvd数据集进行处理
inc_cvd_9 <- inc_cvd %>% 
  rowwise() %>% 
  mutate(inc_cvd_9=case_when(
    inc_chd_9==1|inc_stroke_9==1|inc_hf_9==1|inc_tia_9==1|inc_pvd_9==1|inc_othercvd_9==1~1,
    TRUE~0
  )) %>% 
  mutate(date_cvd_9=ifelse(inc_cvd_9==1,min(c(date_chd_9,date_stroke_9,date_hf_9,date_tia_9,date_pvd_9,date_othercvd_9),na.rm = TRUE),NA
  )) %>% 
  mutate(date_cvd_9=to_date(date_cvd_9)) %>% 
  select(eid,inc_cvd_9,date_cvd_9,inc_chd_9,date_chd_9,inc_stroke_9,date_stroke_9,inc_stroke_i_9,date_stroke_i_9,inc_stroke_h_9,date_stroke_h_9)

#移除合并用到的数据集
rm(inc_chd,inc_stroke,inc_stroke_i,inc_stroke_h,inc_hf,inc_tia,inc_pvd,inc_othercvd,inc_cvd)




###5.1.4（四）# Diagnoses - main ICD9   "41203" "41263" ----

#(1 CHD发病 ICD-9:  410-414,4257
inc_chd <- fun_inc("inc_chd_9_main","date_chd_9_main","41203","41263",28,"^410|^411|^412|^413|^414|^4257")

#(2 卒中发病 430,431,(4330,,4331,4332,4338,4339,4340,4341)
inc_stroke <- fun_inc("inc_stroke_9_main","date_stroke_9_main","41203","41263",28,"^430|^431|^4330|^4331|^4332|^4338|^4339|^4340|^4341")
inc_stroke_i <- fun_inc("inc_stroke_i_9_main","date_stroke_i_9_main","41271","41281",28,"^4330|^4331|^4332|^4338|^4339|^4340|^4341") # ischemic缺血性
inc_stroke_h <- fun_inc("inc_stroke_h_9_main","date_stroke_h_9_main","41271","41281",28,"^430|^431") # hemorrhagic出血性

#(3 心衰发病  428,4020,4021,4029,4040,4041,4049
inc_hf <- fun_inc("inc_hf_9_main","date_hf_9_main","41203","41263",28,"^428|^4020|^4021|^4029|^4040|^4041|^4049")

#(4 TIA发病  4350,4351,4353,4358,3623,4352,4358,4359
inc_tia <- fun_inc("inc_tia_9_main","date_tia_9_main","41203","41263",28,"^4350|^4351|^4353|^4358|^3623|^4352|^4358|^4359")

#(5 PVD发病  4330,4331,4332,4338,4339,4402,7071,7079,4411,4413,4416,4415,4439,4440,4441,4442,4448,4449
inc_pvd <- fun_inc("inc_pvd_9_main","date_pvd_9_main","41203","41263",28,"^4330|^4331|^4332|^4338|^4339|^4402|^7071|^7079|^4411|^4413|^4416|^4415|^4439|^4440|^4441|^4442|^4448|^4449")

#(6 othercvd发病  4340,4341,4349,4432
inc_othercvd <- fun_inc("inc_othercvd_9_main","date_othercvd_9_main","41203","41263",28,"^4340|^4341|^4349|^4432")

#合并
inc_cvd <- full_join(inc_chd,inc_stroke,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_i,by="eid")
inc_cvd <- full_join(inc_cvd,inc_stroke_h,by="eid")
inc_cvd <- full_join(inc_cvd,inc_hf,by="eid")
inc_cvd <- full_join(inc_cvd,inc_tia,by="eid")
inc_cvd <- full_join(inc_cvd,inc_pvd,by="eid")
inc_cvd <- full_join(inc_cvd,inc_othercvd,by="eid")

#对inc_cvd数据集进行处理
inc_cvd_9_main <- inc_cvd %>% 
  rowwise() %>% 
  mutate(inc_cvd_9_main=case_when(
    inc_chd_9_main==1|inc_stroke_9_main==1|inc_hf_9_main==1|inc_tia_9_main==1|inc_pvd_9_main==1|inc_othercvd_9_main==1~1,
    TRUE~0
  )) %>% 
  mutate(date_cvd_9_main=ifelse(inc_cvd_9_main==1,min(c(date_chd_9_main,date_stroke_9_main,date_hf_9_main,date_tia_9_main,date_pvd_9_main,date_othercvd_9_main),na.rm = TRUE),NA
  )) %>% 
  mutate(date_cvd_9_main=to_date(date_cvd_9_main)) %>% 
  select(eid,inc_cvd_9_main,date_cvd_9_main,inc_chd_9_main,date_chd_9_main,inc_stroke_9_main,date_stroke_9_main,inc_stroke_i_9_main,date_stroke_i_9_main,inc_stroke_h_9_main,date_stroke_h_9_main)

#移除合并用到的数据集
rm(inc_chd,inc_stroke,inc_stroke_i,inc_stroke_h,inc_hf,inc_tia,inc_pvd,inc_othercvd,inc_cvd)



###5.1.5  合并发病结果----
#合并
inc_outcome <- full_join(inc_cvd_10,inc_cvd_10_main,by="eid")
inc_outcome <- full_join(inc_outcome,inc_cvd_9,by="eid")
inc_outcome <- full_join(inc_outcome,inc_cvd_9_main,by="eid")


#对inc_outcome数据集进行处理
inc_outcome <- inc_outcome %>% 
  rowwise() %>% 
  #(1. 总cvd发病
  mutate(inc_cvd=case_when(
    inc_cvd_10==1|inc_cvd_10_main==1|inc_cvd_9==1|inc_cvd_9_main==1~1,
    TRUE~0
  )) %>% 
  #总cvd发病时间
  mutate(date_inc_cvd=to_date(ifelse(inc_cvd==1,min(c(date_cvd_10,date_cvd_10_main,date_cvd_9,date_cvd_9_main),na.rm = TRUE),NA))) %>% 
  #(2. 总chd发病
  mutate(inc_chd=case_when(
    inc_chd_10==1|inc_chd_10_main==1|inc_chd_9==1|inc_chd_9_main==1~1,
    TRUE~0
  )) %>% 
  #总chd发病时间
  mutate(date_inc_chd=to_date(ifelse(inc_chd==1,min(c(date_chd_10,date_chd_10_main,date_chd_9,date_chd_9_main),na.rm = TRUE),NA))) %>% 
  #(3.总stroke发病
  mutate(inc_stroke=case_when(
    inc_stroke_10==1|inc_stroke_10_main==1|inc_stroke_9==1|inc_stroke_9_main==1~1,
    TRUE~0
  )) %>% 
  #总stroke发病时间
  mutate(date_inc_stroke=to_date(ifelse(inc_stroke==1,min(c(date_stroke_10,date_stroke_10_main,date_stroke_9,date_stroke_9_main),na.rm = TRUE),NA))) %>% 
  #(4. 缺血性stroke发病
  mutate(inc_stroke_i=case_when(
    inc_stroke_i_10==1|inc_stroke_i_10_main==1|inc_stroke_i_9==1|inc_stroke_i_9_main==1~1,
    TRUE~0
  )) %>% 
  #总stroke发病时间
  mutate(date_inc_stroke_i=to_date(ifelse(inc_stroke_i==1,min(c(date_stroke_i_10,date_stroke_i_10_main,date_stroke_i_9,date_stroke_i_9_main),na.rm = TRUE),NA))) %>% 
  #(5.出血性stroke发病
  mutate(inc_stroke_h=case_when(
    inc_stroke_h_10==1|inc_stroke_h_10_main==1|inc_stroke_h_9==1|inc_stroke_h_9_main==1~1,
    TRUE~0
  )) %>% 
  #总stroke发病时间
  mutate(date_inc_stroke_h=to_date(ifelse(inc_stroke_h==1,min(c(date_stroke_h_10,date_stroke_h_10_main,date_stroke_h_9,date_stroke_h_9_main),na.rm = TRUE),NA))) %>% 
  #选择新构建的变量
  select(eid,inc_cvd,date_inc_cvd,inc_chd,date_inc_chd,inc_stroke,date_inc_stroke,inc_stroke_i,date_inc_stroke_i,inc_stroke_h,date_inc_stroke_h)


#删除已经合并的数据集
rm(inc_cvd_10,inc_cvd_10_main,inc_cvd_9,inc_cvd_9_main)













#6. 从ukb_apply数据集中挑出需要的变量----
# 列表：eid、年龄、性别
ukb_temp <- ukb_apply %>% 
  select(eid,date_bl,date_lost,date_end,age,age_g,sex,race_white,
         cancer_reported,hyp_reported,dm_reported,other_reported,cvd_reported,
         sbp,dbp,med_hyp,hyp,dm,dyslip,bmi,bmi_g3,bmi_g2,
         smoke,drink,edu,physical_activity,sleep_g,diet_score,diet_score_g,
         income_level,employment_status,area,
         fh_cvd,fh_cancer,fh_hyp,fh_dm,
         
         death,date_death,death_chd,death_stroke,death_stroke_i,death_stroke_h,death_hf,death_tia,death_pvd,death_othercvd,death_cvd
         )




#与发病结局数据集合并  
ukb_com<- full_join(ukb_temp,inc_outcome,by="eid")
















#对ukb_com 进行处理
#首先剔除失访--date_lost
#
#


ukb_com <- ukb_com %>% 
  #首先剔除失访--date_lost
  filter(is.na(date_lost)==TRUE) %>% 
  #死亡终点时间
  mutate(py_death=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_cvd=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_chd=to_date(ifelse(death==1,date_death,date_end))) %>%
  mutate(py_death_stroke=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_stroke_i=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_stroke_h=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_hf=to_date(ifelse(death==1,date_death,date_end))) %>%
  mutate(py_death_tia=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_pvd=to_date(ifelse(death==1,date_death,date_end))) %>% 
  mutate(py_death_othercvd=to_date(ifelse(death==1,date_death,date_end))) %>% 
  #发病终点时间
  mutate(py_inc_cvd=to_date(ifelse(inc_cvd==1,date_inc_cvd,date_end))) %>% 
  mutate(py_inc_chd=to_date(ifelse(inc_chd==1,date_inc_chd,date_end))) %>% 
  mutate(py_inc_stroke=to_date(ifelse(inc_stroke==1,date_inc_stroke,date_end))) %>% 
  mutate(py_inc_stroke_i=to_date(ifelse(inc_stroke_i==1,date_inc_stroke_i,date_end))) %>% 
  mutate(py_inc_stroke_h=to_date(ifelse(inc_stroke_h==1,date_inc_stroke_h,date_end))) 
  






#model test
cox_model <- coxph(Surv(py_death-date_bl,death)~age+sex+race_white+hyp+dm+bmi+smoke+drink,data = ukb_com)

summary(cox_model)






##未完成----

#查看缺失情况、填补
a <- aggr(ukb_com,plot = FALSE)
P <- plot(a,numbers=TRUE,prob=FALSE)


########将部分 分类变量->因子化
c_factors <- c('sex','c_smoke','p_smoke','code_noncancer','smoking_status','drinking',
               'heart_diag','dm_diag','can_diag',
               'menopause','n_noncancer','treatment','race',
               'area','work','primary_death','secondary_death','main_diag',
               'secondary_diag','diagnoses')

ukb_apply[c_factors] <- lapply(ukb_apply[c_factors], factor)



































































































