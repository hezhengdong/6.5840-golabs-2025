# 自测脚本

# 执行顺序 mapreduce
# 移除可能存在的输出文件
rm -f mr-out*
# 构建插件
go build -buildmode=plugin ../mrapps/wc.go
# 执行顺序 mapreduce
go run mrsequential.go wc.so pg*.txt
# 将输出存储到另一个文件
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out-0

# 执行自己的代码
go run worker-test.go wc.so
# 将内容放入一个文件中
sort mr-out* | grep . > mr-wc-all

# 对比两文件内容，如果相同则测试通过
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
fi

# 移除此次测试生成的文件
rm -f mr-out* mr-correct-wc.txt mr-wc-all
