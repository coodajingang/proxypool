import os
import time
import sched

### 简单的周期触发爬虫 
### 
def main():
    schedule = sched.scheduler(time.time, time.sleep)
    print("sched start...", time.time())
    while(True):
        print("循环开始：", time.time())
        schedule.enter(2,0, func, ("scrapy crawl xici", time.time()))
        schedule.enter(3,0, func, ("scrapy crawl cloud", time.time()))
        schedule.enter(4,0, func, ("scrapy crawl kuaidaili", time.time()))
        schedule.run()
        print("休眠10分钟 ")
        time.sleep(10*60)

    print("end..", time.time())

def func(cmdstr, starttm):
    now = time.time()
    print("开始处理爬虫： ", cmdstr , " | output=", starttm, " 当前时间：" , now)
    os.system(cmdstr)
    print("爬虫结束： ", cmdstr , " 耗时：" , time.time() - now)



if __name__ == '__main__':
    main()