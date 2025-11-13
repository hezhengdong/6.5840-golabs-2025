#!/usr/bin/env bash

#
# map-reduce tests
# 包含基本功能测试、并行性测试、
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

# 检查操作系统与 Go 版本，跳过可能崩溃的情况
if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

# 检查是否静默执行（静默执行是什么？）
ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}


# timeout 又是干什么？
TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
# 使用顺序 MapReduce 生成正确输出作为对照
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

# 启动协调器进程，后台运行（& 意味着该命令后台异步执行）
maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
pid=$! # &! 是 Shell 的特殊变量，表示最后一个在后台运行的进程 ID

# give the coordinator time to create the sockets.
# 给协调器时间创建 socket
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
# 由于工作器必须在作业完全完成时退出，这意味着作业已完成
sort mr-out* | grep . > mr-wc-all # 合并所有输出文件并过滤空行
# 与正确输出结果进行比较
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait


#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so  &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.
# 测试作业计数
# 输入文件为 8 个 .txt 文件，map 任务会被执行 8 次

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt  &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so

# 命令解释：被反引号``包围的命令会先被执行，然后其输出结果会替换掉整个反引号部分
# 目前理解：因为 key 相同，所以所有 Map 输出都由一个 Reduce 执行，
# 所有输出文件中，只要存在第一行第二列为 8，皆可证明 Map 任务执行了 8 次。
NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
# 测试是否有工作器或协调器在任务完成前提前退出（也就是说，所有的输出文件都已完成）
rm -f mr-*

echo '***' Starting early exit test.

DF=anydone$$ ## 生成一个文件名。$$ 表示当前运行该脚本的进程 ID
rm -f $DF # 确保该文件不存在

(maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt; touch $DF) & # touch 表示创建文件并更新文件的最后修改时间

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ] # 如果文件被创建出来，循环停止
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# 结合前面的脚本与 MapReduce 的目的，如果文件被创建，说明四个节点中存在执行完成的节点。
# 如果 MapReduce 实现正确，那么说明当前任务已经执行完成。
# 将结果排序并输出到指定文件中。
# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
# 等待当前 Shell 的所有后台子进程结束
wait

# 将结果排序并输出到指定文件中
sort mr-out* | grep . > mr-wc-all-final
# 对比两次结果，判断是否存在 MapReduce 作业未完成却提前结束的节点
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test. # 崩溃测试

# generate the correct output
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done # 移除完成标志文件。该文件后续用于判断测试是否完成
((maybe_quiet $TIMEOUT2 ../mrcoordinator ../pg*txt); touch mr-done ) &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

# 循环条件：协调者用于 rpc 通信的 socket 文件存在，且任务完成标志文件不存在
# worker 节点循环崩溃和重启
( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

# 等待当前脚本的所有进程执行完毕
wait

# 清理用于 rpc 通信的 socket 文件
rm $SOCKNAME
# 合并并排序所有输出
sort mr-out* | grep . > mr-crash-all
# 与正确结果做比较，测试工作节点崩溃的情况下，任务是否能成功完成
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
