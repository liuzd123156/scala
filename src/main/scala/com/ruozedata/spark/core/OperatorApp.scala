package com.ruozedata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object OperatorApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("OperatorApp").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)
        //file存储数据格式是test.txt文件中的每一行组成的RDD[String],String示例(网站 流量 时间): www.zhibo8.com	1738	[2018-07-25 13:06:33]
        val file = sc.textFile("D:/test.txt")
        /*
            map(func):对rdd每个元素进行func处理，并返回新的元素，组成新的rdd
         */
        //该map方法处理的结果是RDD[Array[String]],Array[String]示例:Array(www.zhibo8.com,1738,[2018-07-25 13:06:33])
        val mapRdd1 = file.map(_.split("\t"))
        //该map方法处理的结果是RDD[(String,Long)],(String,Long)示例:(www.zhibo8.com,1738)
        val mapRdd2 = file.map(x=>{
            val array = x.split(",")
            (array(0),array(1).toLong)
        })
        //注意：toLong方法要求数据必须是数字组成的可以转换的字符串,需要处理异常数据,即toLong的异常捕捉处理
        val mapRdd3 = file.map(x=>{
            val array = x.split(",")
            var y = 0l
            try{
                y = array(1).toLong
            }catch{
                case e:Exception => e
            }
            (array(0),y)
        })

        /*
            filter(true or false):对rdd每个元素进行判断过滤,结果为true的才会通过,组成新的rdd
         */
        //可以通过文件行split的长度进行过滤,过滤垃圾数据,返回结果和mapRdd1格式一致,数据集中的数量可能会减少
        val filterRdd = mapRdd1.filter(_.size==3)

        /*
            flatMap(func):对每个元素进行map操作之后再进行flat,最后把所有的元素组成新的数据集返回,类似于scala中的map(func)+flatten
            示例数据：
                hello,world,welcome
                hello,welcome
                hello
         */
        val wc = sc.textFile("D:/hive-wc.txt")
        //flatMapRdd的数据结构是RDD[String]
        //数据转变过程如下wc(RDD[String]) ==> wc(Array(String)) ==> flatMapRdd(RDD[String])
        //flatMapRdd示例数据：(hello,world,welcome,hello,welcome,hello)
        val flatMapRdd = wc.flatMap(_.split(","))

        /*
            mapPartitions(func(Iterator[T])):对rdd的每个分区做处理
            和map比较
            优点：
                mapPartitions处理的对象是rdd的分区(即使最后的处理结果还是要落地到每个元素)，该分区的数据是一次加载到内存，从jvm资源的消耗角度来说会节省大量时间和
                空间，所以效率相对map来说会提升很多
            缺点：
                数据是一次加载，如果分区内数据量过大，会出现oom异常
            TestApp.scala中有对比测试代码
         */

        /*
            mapPartitionsWithIndex(func(int,Iterator[T])):功能和mapPartitions一样，只是传入的参数多了一个分区对应的id
         */
        flatMapRdd.mapPartitionsWithIndex((i,x) =>{
            var list = ListBuffer[(Int,String)]()
//            var list2 = List[(Int,String)]()
            while(x.hasNext){
                list.append((i,x.next()))
//                list2 = list2.::(i,x.next())
            }
            list.iterator
        })//.foreach(println)


        /*
            union:返回a和b的并集，不去除重复
         */
        println("---------------------------")
        val a = sc.parallelize(1 to 5)
        val b = sc.parallelize(2 to 4)
        a.union(b).foreach(println)

        /*
            intersection(RDD[T],int):获取a和b的交集，且去除重复元素，第二个参数是指定返回的新的rdd的分区数量
         */
        println("---------------------------")
        a.intersection(b).foreach(println)

        /*
            a.subtract(b):返回a与b的差集，不去重复，第二个参数是指定返回的新的rdd的分区数量
         */
        println("---------------------------")
        a.subtract(b).foreach(println)

        /*
            distinct(int):返回去重的rdd，第二个参数是指定返回的新的rdd的分区数量
         */
        println("---------------------------")
        a.union(b).distinct().foreach(println)

        /*
            groupBy():根据指定的分组依据对rdd所有元素进行分组
                def groupBy[K: ClassTag](f: T => K): RDD[(K, Iterable[T])]
                def groupBy[K: ClassTag](f: T => K, numPartitions: Int): RDD[(K, Iterable[T])]
                def groupBy[K: ClassTag](f: T => K, p: Partitioner): RDD[(K, Iterable[T])]
            groupByKey:算是groupBy的定制版本，只能接收键值对(tuple(key,value)和map)
                def groupByKey(): RDD[(K, Iterable[V])]
                def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
                def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
            注意：groupBy返回的是(key,(元素集合组成的tuple))，而groupByKey返回的是(key,(value集合组成的tuple))
                groupByKey需要把多个分区的数据都统一加载到内存中，如果某个key对应的数据过多，会导致oom异常，reduceByKey会现在分区内进行聚合，然后再统一聚合
         */
        //根据奇偶分组
        println("---------------------------")
        a.groupBy(x => {if (x%2==0) 0 else 1}).foreach(println)
        println("---------------------------")
        a.map(x=>{
            var key = 0
            if(x%2!=0){
                key = 1
            }
            (key,x)
        }).groupBy(_._1).foreach(println)
        println("---------------------------")
        a.map(x=>{
            var key = 0
            if(x%2!=0){
                key = 1
            }
            (key,x)
        }).groupByKey().foreach(println)
        println("---------------------------")

        /*
            reduceByKey:根据key进行value聚合操作，要求rdd必须为(key,value)
            该算子会对分区先行计算，在汇总计算
         */
        flatMapRdd.map((_,1)).reduceByKey((x,y)=>{x+y}).foreach(println)

        /*
            mapValues:只针对value进行方法操作，key不做变化
         */
        sc.parallelize(List(("a",1),("b",2),("c",3))).mapValues(_+1)
        //返回结果是(("a",2),("b",3),("c",4))

        /*
            aggregateByKey(transformation):相当于是针对不同“key”数据做一个map+reduce规约的操作
            aggregateByKey(参数1)(参数2，参数3)：
            参数1为初始化值
            在参数2的函数中(可以理解为map)，初始值和该key的每一个value传入函数进行操作,然后结果和下一个元素操作，和map不同的是最终只返回初始值类型的结果
            所有返回的结果在参数3中进行规约
         */
        //需求：针对用户聚合用户的访问记录
        val data = sc.parallelize(
            List(("13909029812",("20170507","http://www.baidu.com")),
                ("18089376778",("20170401","http://www.google.com")),
                ("18089376778",("20170508","http://www.taobao.com")),
                ("13909029812",("20170507","http://www.51cto.com")))
        )
        //(Set[(String, String)]定义参数1)((set:参数1，item:key对应的value),(set1, set2)：参数3，参数2返回的set集合)
        data.aggregateByKey(scala.collection.mutable.Set[(String, String)](), 200)((set, item) => {
            println("seq"+set.mkString("[",",","]")+"---"+item)
            set += item
        }, (set1, set2) => {
            println("seq"+set1.mkString("[",",","]")+"---"+set2.mkString("[",",","]"))
            set1 union set2}).mapValues(x => x.toIterable).collect

        /*
            sortBy(f: (T) => K,ascending: Boolean = true,numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K])
                根据指定位置排序，参数1：指定排序依据，参数2：指定升序或者降序，默认升序，参数3：排序返回的分区数
                参数4：自定义排序方法
            sortByKey([ascending], [numPartitions]):根据key进行排序
            注意：两者都是基于分区内排序，如果numPartitions指定为1，代表全局排序
         */
        val datas = sc.parallelize(Array(("cc",12),("bb",32),("cc",22),("aa",18),("bb",16),("dd",16),("ee",54),("cc",1),("ff",13),("gg",32),("bb",4)))
        val counts = datas.reduceByKey(_+_)
        // 按照value进行降序排序
        val sorts = counts.sortBy(_._2,false)
        sorts.collect().foreach(println)

        /*
            先按照第一个，再按照第三个元素进行升序排序
            输出结果：
                (1,1,2)
                (1,3,5)
                (1,6,3)
                (2,1,2)
                (2,3,3)
         */
        val arr = Array((1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2))
        val sorts2 = sc.parallelize(arr,1).sortBy(e => (e._1,e._2))
        sorts2.foreach(println)

        /*
            (gg,68)
            (ff,13)
            (ee,104)
            (dd,16)
            (cc,32)
            (cc,22)
            (cc,1)
            (bb,32)
            (bb,6)
            (bb,44)
            (aa,18)
         */
        val d2 = sc.parallelize(Array(("cc",32),("bb",32),("cc",22),("aa",18),("bb",6),("dd",16),("ee",104),("cc",1),("ff",13),("gg",68),("bb",44)))
        d2.sortByKey(false,1).foreach(println)

        /*
            join:连接两个RDD[(key,value)],返回两个RDD共同拥有的key对应的value组成的tuple
            RDDA[(key,value)].join(RDD[(key2,value2)])==>(key,(value,value2))
            注意：该算子类似于sql中的inner join，只返回两个rdd共有的key
            返回结果：
            Array[(Int, (Int, Int))] = Array((4,(1,1)), (2,(1,1)), (3,(1,1)))
         */
        val aa = sc.parallelize(Array(1,2,3,4,5))
        val bb = sc.parallelize(Array(2,3,4)).map((_,1))
        aa.map((_,1)).join(bb).collect

        /*
            cogroup(也叫groupWith):效果类似于group之后的两个rdd再进行join操作，但是join返回key是左右join的合集
            ((1,1),(2,2)).cogroup(((1,1),(3,3)))==>((1,(CompactBuffer(1),CompactBuffer(1))),(2,(CompactBuffer(2),CompactBuffer())),(3,(CompactBuffer(),CompactBuffer(3))))
            输出结果：
                (1,(CompactBuffer(1),CompactBuffer()))
                (3,(CompactBuffer(1),CompactBuffer(1)))
                (5,(CompactBuffer(1),CompactBuffer()))
                (4,(CompactBuffer(1),CompactBuffer(1)))
                (6,(CompactBuffer(),CompactBuffer(1)))
                (2,(CompactBuffer(1),CompactBuffer(1)))
         */
        val aaa = sc.parallelize(Array(1,2,3,4,5))
        val bbb = sc.parallelize(Array(2,3,4,6)).map((_,1))
        aaa.map((_,1)).cogroup(bbb).foreach(println)

        /*
            cartesian:求两个rdd的元素的笛卡尔积,每两个元素组成一个tuple
         */
        val aaaa = sc.parallelize(Array(1,2,3,4,5))
        val bbbb = sc.parallelize(Array(2,3,4,6))
        aaaa.cartesian(bbbb).collect()

        /*
            repartition:源rdd重新分区产生新的rdd，内部调用的是coalesce(shuffle=true)
            coalesce:shuffle默认是false
            注意：当需要减少并行度的时候需使用coalesce，这是个窄依赖，不会产生shuffle(默认才行)
            分区由少变多，或者在一些不是键值对的rdd中想要重新分区的话，就需要使用repartition了
            如果是需要增加并行度repartition和coalesce(true)差不多
         */
        /*
            aggregate(action)：初始值和分区内第一个元素做计算，然后结果再和下一个元素做计算(即第二个参数)，直到当前分区计算完成，然后重新开始下一个分区
                        所有分区经过第二个参数处理完毕后，再对处理的结果进行第三个参数的计算，也是从初始值开始(aggregateByKey的第三个参数就没有初始值参与了)
            返回结果：
            seq:0   4
            seq:0,4 5
            seq:0,4,5       6
            seq:0   1
            seq:0,1 2
            seq:0,1,2       3
            comb:0  0,1,2,3
            comb:0,0,1,2,3  0,4,5,6
            res1: String = 0,0,1,2,3,0,4,5,6
         */
        sc.parallelize(List(1,2,3,4,5,6),2).aggregate("0")((a,b)=>{
            println("seq:"+a+"\t"+b)
            a+","+b},(x,y)=>{
            println("comb:"+x+"\t"+y)
            x+","+y}
        )
        sc.stop()
    }
}
