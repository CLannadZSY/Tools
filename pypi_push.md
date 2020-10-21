[Pypi 发布步骤]
1. 创建 `setup.py`, 详细配置参考官方文档

2. ```bash
   # setup.py 同级目录执行
   python3 setup.py sdist bdist_wheel
   ```

3. ```bash
   python3 -m pip install twine
   ```

4. ```bash
   python3 -m twine upload dist/*
   # 需要输入 Pypi 已注册的帐号和密码
   ```

5. 免输入帐号密码, 进行上传

   创建 `.pypirc`,  保存到  `~/` 目录下

   ```ini
   [distutils]
   index-servers = pypi
   
   [pypi]
   username:__token__
   password:帐号的 API token
   ```

