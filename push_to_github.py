# -*- coding: utf-8 -*-
# ## 推送主分支

# !git config --global user.email "lyhue1991@163.com"
# 出现一些类似 warning: LF will be replaced by CRLF in <file-name>. 可启用如下设置。
# !git config --global core.autocrlf false
# 配置打印历史commit的快捷命令
# !git config --global alias.lg "log --oneline --graph --all"

# !git init

# !git add  ./data/*  *.md *.py

# +
# #!rm -rf *.html
# -

# !git rm --cached  .ipynb_checkpoints/* 

# !git commit -m"add some chapters"

# !git remote add origin https://github.com/lyhue1991/eat_pyspark_in_10_days

# !git pull origin master 

# !git push  origin master 

# !git remote rm gitee 

# !git remote add gitee https://gitee.com/Python_Ai_Road/eat_pyspark_in_10_days

# !git push -f gitee master 

# ## 创建pages分支

# !git checkout -b gh-pages

# !git rm --cached -r *.md

# !git clean -df
# !rm -rf *.md

# !cp -r _book/* .

# !git add .

# !git reset

# !git pull origin gh-pages

# !git commit -m 'add gh-pages'

# !git push -u origin gh-pages

# !git checkout pages
