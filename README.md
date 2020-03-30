    
    IP画像私有化部署SDK， 支持数据库：mysql，mongodb
        
    目录结构：
        |__config.py         # 配置项
        |__utls.py           # 解析入库的一些方法
        |__demo_mongo.py     # 入mongodb库的启动脚本
        |__demo_mysql.py     # 入mysql库的启动脚本
        |__requirements.txt  # python3 的一些依赖包
        
        
    备注： 脚本执行用户 需要有当前项目下mkdir&rm的权限
    
          mysql 用户需要有库的读写权限，创建表的权限
          
          
    部署步骤：
    
    一. 环境准备
        linux， python3.6, mongo4.0/mysql5.6（任意一种）
        
        安装依赖包 pip isntall -r requirements.txt
    
    二. 启动任务
        后台执行：nohup pyhton demo_mysql.py &  或者选择supervisor启动任务   
                 