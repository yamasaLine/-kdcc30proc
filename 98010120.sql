/*
*author:zhangyuchun
*description:股票审核
*adddate:2015-06-10
*editdate:2015-07-10  添加对总的了结笔数和总的成功率的统计
		  2015-07-13  透支数据根据高手类型统计
		  2015-07-20  修复不涉及扣除或回补积分的时候对确认消息的判断
		  2015-07-20  修复回补积分会产生消费积分的bug
		  2015-07-22  修复账户扣除为负数的bug
		  2015-07-22  添加审核未通过的逻辑
		  2015-07-22  修复账户扣除为负数的bug
		  2015-07-23  修复回补积分未更新totalcash的bug
		  2015-07-30  修复交易失败后，将交易记录置为驳回
		  2015-08-19  添加仓位的统计；把自然日的统计过程移入up_mid_98020390 by zhoujia
		  2015-11-25  历史消息、订阅消息和高手操作表，增加持仓天数和持仓 add by zhoujia
		  2015-12-17  卖出操作后会更新最高收益率 add by zhoujia
		  2015-12-28  交易后将消费流水写到新的统一积分流水表 add by zhangyuchun
		  2015-01-07  只有有消费的时候才记录消费流水 add by zhangyuchun
		  2016-04-26  修复计算积分的bug add by zhangyuchun
*/
ALTER procedure [dbo].[up_mid_98010120]
@id int, --需要审核的交易id
@p_gybh int=0, --操作员工编号
@examine varchar(10)='' --审核结果
as
begin
--声明交易数据变量
declare @gsid int --策略id
declare @gsname varchar(100) --策略名称
declare @msgid int --生成的高手消息id，根据高手消息id生成订阅消息
declare @stockcode varchar(50) --股票代码
declare @stockname varchar(30) --股票名称
declare @createdate varchar(30) --创建时间
declare @tradetime varchar(30) --交易时间
declare @operate int --操作方向，1：买入；2：卖出；
declare @operate_str varchar(30) --买入；卖出
declare @price numeric(19,2) --股票价格
declare @shuliang int --股票数量
declare @successrate numeric(19,4) --成功率
declare @selltotal int --了结笔数
declare @profit numeric(19,4) --收益
declare @position numeric(10,2) --每笔交易的仓位
set @position = 0;
declare @positionDays int --持仓天数
set @positionDays = 0;
------------------------------判断当前审核的id的数据是否存在------------------------------
set @gsid=0
select @gsid=gsid,@gsname=gsname,@stockcode=stockcode,@stockname=stockname,@createdate=createdate,@tradetime=jydatetime,
@operate=operate,@shuliang=shuliang,@price=price from kdcc30data..t_cl_gs_stock where id=@id
if @gsid<1
	begin
		select errorcode=-1,errormsg='未找到交易记录'
		select errorcode=-1,errormsg='未找到交易记录'
		return
	end
if @operate=1
	set @operate_str='买入'
else
	set @operate_str='卖出'
declare @step int --当前执行的步骤id
--默认第一步
set @step=1
------------------------------判断是否有审核日志记录------------------------------
if not exists(select 1 from kdcc30data..t_cl_stock_log where [sid]=@id)
	begin
		--插入执行记录
		insert into kdcc30data..t_cl_stock_log ([sid],createid,createtime,updateid,updatetime,step)
		values (@id,@p_gybh,getdate(),@p_gybh,getdate(),@step)
	end
--查询当前执行的步骤
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
--如果当前执行的步骤大于或等于制定的步骤数，则表示当前交易记录审核环节都已经执行完毕，则直接返回消息提示
declare @maxstep int
declare @msg varchar(100)
select @maxstep=count(*) from kdcc30data..t_cl_stock_step
if @step>=@maxstep
	begin
		select errorcode=100,errormsg='已经审核完毕'
		select errorcode=100,errormsg='已经审核完毕'
		return
	end
--如果是审核未通过，直接更新状态且记录审核日志
if @examine='2'
	begin
		--执行审核交易记录的操作
		 update kdcc30data..t_cl_gs_stock set status=2 where id=@id
		 --添加审核日志
		 update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=@maxstep where [sid]=@id
		 select errorcode=100,errormsg='审核成功'
		 select errorcode=100,errormsg='审核成功'
		 return
	end
--只有在步骤2才会判断交易规则
if @step<=2
	begin
		------------------------------交易规则判断------------------------------
		--买入规则判断
		if @operate=1
			begin
				if exists (select 1 from kdcc30data..t_cl_gs_khzzh where cash<convert(numeric(19,2),@price)*@shuliang and gsid=@gsid)
					begin
						--添加审核日志
						--更新交易记录为驳回
						update kdcc30data..t_cl_gs_stock set status=2 where id=@id
						update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),@step=@maxstep,msg='高手资产值不够，购买失败' where [sid]=@id
						select errorcode=-1,errormsg='高手资产值不够，购买失败!'
						select errorcode=-1,errormsg='高手资产值不够，购买失败!'
						return
					end
			end
			--卖出规则判断
			else
				begin
					--看是否有持仓，如果没有持仓，则卖出失败
					if not exists(select 1 from kdcc30data..t_cl_gs_chicang where stockcode=@stockcode and gsid=@gsid)
						begin
							--添加审核日志
							update kdcc30data..t_cl_gs_stock set status=2 where id=@id
							update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=@maxstep,msg='高手没有持仓，卖出失败' where [sid]=@id
							select errorcode=-1,errormsg='高手没有持仓，卖出失败!'
							select errorcode=-1,errormsg='高手没有持仓，卖出失败!'
							return
						end
					if exists(select 1 from kdcc30data..t_cl_gs_chicang where stockcode=@stockcode and stockbal<@shuliang and gsid=@gsid)
						begin
							--添加审核日志
							update kdcc30data..t_cl_gs_stock set status=2 where id=@id
							update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=@maxstep,msg='高手持仓不够，卖出失败' where [sid]=@id
							select errorcode=-1,errormsg='高手持仓不够，卖出失败!'
							select errorcode=-1,errormsg='高手持仓不够，卖出失败!'
							return
						end
				end
	end
------------------------------审核交易记录(step=1)------------------------------
if @step=1
begin
	 --执行审核交易记录的操作
	 update kdcc30data..t_cl_gs_stock set status=1 where id=@id
	 --添加审核日志
	 update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=2 where [sid]=@id
end
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
------------------------------资产与持仓的更新(step=2)------------------------------
if @step=2
begin

	-- 获取更新资产前的值
	declare @cash numeric(19,4)
	declare @onestockvalue numeric(19,4)--单支股票总资产
	set @onestockvalue = 0
	declare @stockvalue numeric(19,4)--持仓总资产
	set @stockvalue = 0
	declare @currentvalue numeric(19,4)

	select @cash=cash from kdcc30data..t_cl_gs_khzzh where gsid=@gsid --买入或卖出前持有的现金
	--获取持仓总额
	select @stockvalue=sum(temp.value)
	from(
		select gsid, stockcode, sum(costprice*stockbal) as value
		from kdcc30data..t_cl_gs_chicang
		group by gsid,stockcode) as temp
	where temp.gsid=@gsid
	group by temp.gsid
	set @currentvalue = @price * @shuliang --买入或卖出单支股票的总额
	--设置总资产
	declare @allvalue numeric(19,4)--总资产
	set @allvalue = @cash + isnull(@stockvalue,0)

	--买入
	if @operate=1
		begin

			--更新资产
			update kdcc30data..t_cl_gs_khzzh set cash=cash-@price*@shuliang where gsid=@gsid

			--添加或更新持仓
			if exists (select 1 from kdcc30data..t_cl_gs_chicang where gsid=@gsid and stockcode=@stockcode)
				begin
					--成本价格=((历史成本价格*历史持仓数)+(买入价格*买入数量))/(历史持仓数+买入数量)
					update kdcc30data..t_cl_gs_chicang set stockbal=stockbal+@shuliang,
						costprice=((costprice*stockbal)+(@price*@shuliang))/(stockbal+@shuliang)
						where gsid=@gsid and stockcode=@stockcode

					--add by zhoujia:2015-08-19
					--找出已持仓的同种股票总价
					select @onestockvalue=sum(costprice*stockbal)
					from kdcc30data..t_cl_gs_chicang as chicang
					where gsid=@gsid and stockcode=@stockcode
					group by gsid,stockcode

					--更新单支股票仓位
					set @position = @onestockvalue*100/@allvalue;
					update kdcc30data..t_cl_gs_chicang set position=@position
					where gsid=@gsid and stockcode=@stockcode

				end
			else
				begin
					insert into kdcc30data..t_cl_gs_chicang(gsid,stockcode,stockname,stockbal,costprice,dateint)
						values(@gsid,@stockcode,@stockname,@shuliang,@price,convert(varchar(8),getDate(),112))

					--add by zhoujia:2015-08-19
					--更新单支股票仓位
					set @position = @currentvalue*100/@allvalue;
					update kdcc30data..t_cl_gs_chicang set position = @position
					where gsid=@gsid and stockcode=@stockcode

				end
		end
	--卖出
	else
		begin
			--更新资产
			update kdcc30data..t_cl_gs_khzzh set cash=cash+@price*@shuliang where gsid=@gsid
			--计算收益
			declare @costprice numeric(19,4)
			select @costprice=isnull(costprice,0) from kdcc30data..t_cl_gs_chicang where gsid=@gsid and stockcode=@stockcode
			set @profit=(@price-@costprice)*100/@costprice
			update kdcc30data..t_cl_gs_stock set profit=@profit where id=@id
			--更新最大收益率
			declare @topProfit numeric(19,4);
			set @topProfit = (select topprofit from kdcc30data..t_cl_gs_khzzh where gsid=@gsid);
			if(@profit > @topProfit)
			  begin
			    update kdcc30data..t_cl_gs_khzzh set topprofit = @profit where gsid=@gsid;
			  end

			--删除或更新持仓
			declare @chicangStartDay varchar(10); --持仓起始时间 add by zhoujia:2015-11-25
			if not exists (select 1 from kdcc30data..t_cl_gs_chicang where gsid=@gsid and stockcode=@stockcode and stockbal=@shuliang)
				begin
					--成本价格=((历史成本价格*历史持仓数)+(买入价格*买入数量))/(历史持仓数+买入数量)
					update kdcc30data..t_cl_gs_chicang set stockbal=stockbal-@shuliang,
						costprice=((costprice*stockbal)-(@price*@shuliang))/(stockbal-@shuliang)
						where gsid=@gsid and stockcode=@stockcode

					--add by zhoujia:2015-08-19
					--找出已持仓的同种股票总价
					select @onestockvalue=sum(costprice*stockbal),
					  @chicangStartDay=cast(chicang.dateint as varchar(10))
					from kdcc30data..t_cl_gs_chicang as chicang
					where gsid=@gsid and stockcode=@stockcode
					group by gsid,stockcode,dateint
					--更新单支股票仓位
					set @position = @onestockvalue*100/@allvalue;
					update kdcc30data..t_cl_gs_chicang set position=@position
					where gsid=@gsid and stockcode=@stockcode;

					--计算持仓天数 add by zhoujia:2015-11-25
					set @positionDays = datediff(day, @chicangStartDay, convert(varchar(8), getdate(),112));

				end
			else
				begin
					--如果卖出的数量与持仓的数量一致，删除持仓
					--计算持仓天数 add by zhoujia:2015-11-25
					select @chicangStartDay=cast(chicang.dateint as varchar(10))
					from kdcc30data..t_cl_gs_chicang as chicang
					where gsid=@gsid and stockcode=@stockcode;

					set @positionDays = datediff(day, @chicangStartDay, convert(varchar(8), getdate(),112))
					delete from kdcc30data..t_cl_gs_chicang where gsid=@gsid and stockcode=@stockcode
				end
			-- 更新高手操作,持仓天数
			update kdcc30data..t_cl_gs_stock set positiondays=@positionDays, position=@position where id=@id
		end

	-- 更新总仓位 （A股票现价*A股票股数+B股票现价*B股票股数）/总资产
	-- 总资产 = A股票现价*A股票股数 + B股票现价*B股票股数+剩余现金
	if exists (select 1 from kdcc30data..t_cl_gs_chicang where gsid=@gsid)
		begin
			select @stockvalue=isnull(sum(temp.value),0)
			from(
				select gsid, stockcode, sum(costprice*stockbal) as value
				from kdcc30data..t_cl_gs_chicang
				group by gsid,stockcode) as temp
			where temp.gsid=@gsid
			group by temp.gsid
		end
	else
		begin
			set @stockvalue=0
		end

	update kdcc30data..t_cl_gs_khzzh set position=isnull(@stockvalue,0) * 100/@allvalue where gsid=@gsid

	--添加审核日志
	update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=3 where [sid]=@id
end
------------------------------数据统计(step=3)------------------------------
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
if @step=3
	begin
		--当月数据统计
		declare @monthint varchar(6)
		select @monthint=isnull(monthint,0) from kdcc30data..t_cl_gs_khzzh where gsid=@gsid
		--如果是新的一月，更新月统计数据
		if @monthint!=convert(varchar(6),getdate(),112)
			begin
				--清空月统计数据
				update kdcc30data..t_cl_gs_khzzh set monthsuccessrate=0,
						monthsuccesstotal=0,monthselltotal=0,monthdeals=0,monthsells=0,monthbuys=0,monthint=convert(varchar(6),getdate(),112)
						where gsid=@gsid
			end
		--更新买入月统计数据
		if @operate=1
			begin
				--更新操作次数,共买入次数，共操作次数，共买入次数
				update kdcc30data..t_cl_gs_khzzh set
					monthdeals=monthdeals+1,monthbuys=monthbuys+1,totaldeals=isnull(totaldeals,0)+1,totalbuys=isnull(totalbuys,0)+1
					where gsid=@gsid
			end
		if @operate=2
			begin
				update kdcc30data..t_cl_gs_khzzh set
					monthselltotal=monthselltotal+1,monthdeals=monthdeals+1,monthsells=monthsells+1,totaldeals=isnull(totaldeals,0)+1,
					totalsells=isnull(totalsells,0)+1,selltotal=isnull(selltotal,0)+1 where gsid=@gsid
				--如果收益大于0，更新成功笔数和成功率
				if @profit>0
					begin
						update kdcc30data..t_cl_gs_khzzh set monthsuccesstotal=monthsuccesstotal+1,successtotal=successtotal+1 where gsid=@gsid
						update kdcc30data..t_cl_gs_khzzh set monthsuccessrate=monthsuccesstotal*100/monthsells,successrate=(successtotal*100)/totalsells where gsid=@gsid
					end
			end
		--添加审核日志
		update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=4 where [sid]=@id
	end
------------------------------近XX个自然日数据统计(step=4)------------------------------
------------------------------根据用户类型决定统计多少个自然日的数据------------------------------
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
if @step=4
	begin
		--只在卖出才更新统计数据
		if @operate=2
		begin
			-- 每次卖出操作完，执行一次高手字段统计 add by zhoujia
			exec up_mid_98020390
				 @gsid=@gsid,
				 @isinner=1
		end

		----------计算近三十日统计数据结束
		--添加审核日志
		update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=5 where [sid]=@id
	end
------------------------------高手消息生成(step=5)-----------------------------
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
declare @gsmsgid int --高手消息id
set @gsmsgid=0
--查询是否已经生成了高手消息id
select @gsmsgid=id,@profit=profit from kdcc30data..t_cl_gsmess where
		gsid=@gsid and gsname=@gsname and stockprice=@price and stockcode=@stockcode
		and stockname=@stockname and tradetime=@createdate and stockprice=@price and stocknumber=@shuliang
--根据分类查询消息是否需要确认
declare @r_msgconfirm int
declare @r_gstype varchar(36)
select @r_msgconfirm=isnull(msgconfirm,0),@r_gstype=isnull(gs.gstype,'') from kdcc30data..t_cl_gs gs left join kdcc30data..t_cl_gs_type t
		on gs.gstype=t.id where gsid=@gsid
if @r_msgconfirm=0
	set @r_msgconfirm=null
if @step=5
	begin
		--如果未生成高手消息，生成高手消息
		if @gsmsgid=0
			begin
				--生成高手消息id
				insert into kdcc30data..t_cl_gsmess
					(gsstock_id,gsid,gsname,tradetime,stockprice,stocknumber,stockcode,stockname,datatype, ordertype,content,markcontent,dateint,createdate,profit,msgconfirm,position,positiondays)
					values(@id,@gsid,@gsname,@createdate,@price,@shuliang,@stockcode,@stockname,'成交',@operate_str,'在'+convert(varchar
		(50),@price)+'元价格委托'+@operate_str+convert(varchar(50),@shuliang)+'股'+@stockname+'('+@stockcode+')',
					'在'+convert(varchar(50),@price)+'元价格委托'+@operate_str+convert(varchar(50),@shuliang)+'股*****',convert(varchar(8),getDate(),112),getDate(),@profit,@r_msgconfirm,@position,@positionDays)
				select @gsmsgid=@@identity
			end
		--添加审核日志
		update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),step=6 where [sid]=@id
	end
------------------------------订阅消息生成(step=6)-----------------------------
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
if @step=6
begin
	--进入生成订阅消息的步骤，更新状态，避免重复进入
	if exists(select 1 from kdcc30data..t_cl_stock_log where [sid]=@id and khmsg=1)
		begin
			select errorcode=-1,errormsg='消息正在生成中'
			select errorcode=-1,errormsg='消息正在生成中'
			return
		end
	--更新状态
	update kdcc30data..t_cl_stock_log set msgstarttime=getdate(),khmsg=1 where [sid]=@id
	--生成订阅消息
	--查询对应的积分规则
	declare @cashRuleId varchar(36)
	select @cashRuleId=isnull(rid,'') from kdcc30data..t_cl_gs where gsid=@gsid
	if @cashRuleId=''
		begin
			--查询对应分类的积分规则
			select @cashRuleId=t.rid from kdcc30data..t_cl_gs gs inner join kdcc30data..t_cl_gs_type t
			on gs.gstype=t.id where gsid=@gsid
		end
	--如果未查询到积分规则，则不继续执行
	if not exists(select 1 from kdcc30data..t_cl_point_rule where id=@cashRuleId)
		begin
			update kdcc30data..t_cl_stock_log set khmsg=0,msg='未找到积分规则',updateid=@p_gybh,updatetime=getdate() where [sid]=@id
			select errorcode=-1,errormsg='未找到积分规则'
			select errorcode=-1,errormsg='未找到积分规则'
			return
		end
	--通过验证，查询对应的积分规则
	--定义规则变量
	declare @r_profit numeric(19,4)
	declare @r_point int
	declare @r_inpro int
	declare @r_inpoint int
	declare @r_overcount int
	declare @r_overpoint int
	select @r_profit=profit,@r_point=isnull(point,0),@r_inpro=isnull(inpro,0),@r_inpoint=isnull(inpoint,0),
	@r_overcount=isnull(overcount,0),@r_overpoint=isnull(overpoint,0) from kdcc30data..t_cl_point_rule where id=@cashRuleId
	if @r_overpoint>0
	set @r_overpoint=@r_overpoint*-1
	--如果没有透支积分设置，则设置-10000000，使该条件无效
	if @r_overpoint=0
		set @r_overpoint=-10000000
	if @gsmsgid=0
		begin
			update kdcc30data..t_cl_stock_log set khmsg=0,msg='未找到对应的消息id',updateid=@p_gybh,updatetime=getdate() where [sid]=@id
			select errorcode=-1,errormsg='未找到对应的消息id'
			select errorcode=-1,errormsg='未找到对应的消息id'
			return
		end
	--如果不涉及到扣除或回补积分，则直接生成消息
	if @operate=1
		begin
			--如果不允许透支，则所有关注的用户都会发送消息
			declare @basePoint int
			--如果不允许透支，则必须大于消耗的基础积分
			begin try
			begin tran
				--允许透支
				if @r_overcount>0
					begin
						insert into kdcc30data..t_cl_wxmess (gsid,gsmessid,profit,gsname,jswxid,khid,stockcode,stockname,
																stocknumber,stockprice,datatype,ordertype,tradetime,content,createdate,buytype,valuecash,cash,cashtype,dateint,msgconfirm,position,positiondays)
						select msg.gsid,msg.id,msg.profit,msg.gsname,kh.wxid,kh.khid,msg.stockcode,msg.stockname,
																msg.stocknumber,msg.stockprice,msg.datatype,msg.ordertype,msg.tradetime,msg.content,getdate(),
																gskh.subtype,0,0,0,convert(varchar(8),getDate(),112),@r_msgconfirm,@position,@positionDays
																 from (select * from kdcc30data..t_cl_gs_kh where gsid=@gsid and status=1) gskh
																inner join kdcc30data..t_cl_kh kh on gskh.khid=kh.khid
																inner join kdcc30data..t_cl_kh_config c on gskh.khid=c.khid
																inner join kdcc30data..t_cl_gsmess msg on gskh.gsid=msg.gsid
																where kh.status=0 and msg.id=@gsmsgid and kh.wxid!=''
					end
				--禁止透支
				else
					begin
						insert into kdcc30data..t_cl_wxmess (gsid,gsmessid,profit,gsname,jswxid,khid,stockcode,stockname,
																stocknumber,stockprice,datatype,ordertype,tradetime,content,createdate,buytype,valuecash,cash,cashtype,dateint,msgconfirm,position,positiondays)
						select msg.gsid,msg.id,msg.profit,msg.gsname,kh.wxid,kh.khid,msg.stockcode,msg.stockname,
																msg.stocknumber,msg.stockprice,msg.datatype,msg.ordertype,msg.tradetime,msg.content,getdate(),
																gskh.subtype,0,0,0,convert(varchar(8),getDate(),112),@r_msgconfirm,@position,@positionDays
																 from (select * from kdcc30data..t_cl_gs_kh where gsid=@gsid and status=1) gskh
																inner join kdcc30data..t_cl_kh kh on gskh.khid=kh.khid
																inner join kdcc30data..t_cl_kh_config c on gskh.khid=c.khid
																inner join kdcc30data..t_cl_gsmess msg on gskh.gsid=msg.gsid
																where kh.status=0 and msg.id=@gsmsgid and kh.wxid!=''
																and c.leftcash>=@r_point
					end

				update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),msgendtime=getdate(),msgcount=@@rowcount,step=7 where [sid]=@id
				commit tran
			end try
			begin catch
				if @@TRANCOUNT>0
					begin
						rollback tran
					end
				--记录异常
				update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),khmsg=0,msg=ERROR_MESSAGE() where [sid]=@id
				select errorcode=-1,errormsg='生成消息出现异常'
				select errorcode=-1,errormsg='生成消息出现异常'
				return
			end catch
		end
	--如果涉及到扣除或回补积分，计算积分
	else
		begin
			--计算该条消息产生的消费积分
			declare @msgCash int
			set @msgCash=0
			--当前盈利
			declare @currentProfit int
			set @currentProfit=convert(int,@profit)
			if @currentProfit<0
				set @currentProfit=-1*@currentProfit
			--计算应该扣除的积分
			if @currentProfit>=@r_profit
				begin
				set @msgCash=@r_point
				--计数器
				declare @index int
				set @index=1
				set @currentProfit=@currentProfit-@r_profit
				while(@currentProfit>0 and @r_inpro>0)
					begin
						if @currentProfit-@r_inpro>0
							begin
								set @msgCash=@msgCash+(@r_inpoint*@index*@r_inpro)
							end
						else
							begin
								set @msgCash=@msgCash+(@currentProfit*@index*@r_inpoint)
							end
						--进入下一个区间判断
						--修改计算积分的bug 2016-04-25 zhangyuchun
						set @currentProfit=@currentProfit-@r_inpro
						set @index=@index+1
					end
				end
				--结果积分
				--保存计算出来的积分结果
				update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),point=@msgCash where [sid]=@id
				--分别生成消息
				--查询最后一次买入该股票的交易记录
				declare @buyMsgId int
				--消息确认
				declare @buyMsgConfirm int
				set @buyMsgId=0
				select @buyMsgId=b.id,@buyMsgConfirm=isnull(b.msgconfirm,0) from (select top 1 * from kdcc30data..t_cl_gs_stock(nolock) where operate=1 and status=1 and gsid=@gsid and stockcode=@stockcode
order by jydatetime desc)
a inner join kdcc30data..t_cl_gsmess b(nolock) on a.gsid=b.gsid and a.stockcode=b.stockcode and a.jydatetime=b.tradetime
				if @buyMsgId=0
					begin
						update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),khmsg=0,msg='未找到买入交易记录' where [sid]=@id
						select errorcode=-1,errormsg='未找到买入交易记录'
						select errorcode=-1,errormsg='未找到买入交易记录'
					end
				begin try
				begin tran
					--只有收到买入消息才会发送卖出消息
					--声明客户消息相关变量
					declare @m_khid int --客户id
					declare @m_khtype int --客户类型
					declare @m_subtype int --订阅类型
					declare @m_wxid varchar(50) --微信id
					declare @m_leftcash int --总剩余积分
					declare @m_overcount int --透支次数
					declare @m_overpoint int --透支分数
					declare @m_cash int --剩余现金积分
					declare khCursor cursor local for
						select kh.khid,kh.wxid,kh.khtype,gskh.subtype,c.leftcash,isnull(ol.overcount,0) overcount,isnull(ol.overpoint,0) overpoint
						,cc.leftcash cash
							from kdcc30data..t_cl_gs_kh gskh(nolock)
							inner join kdcc30data..t_cl_kh kh(nolock) on gskh.khid=kh.khid
							inner join kdcc30data..t_cl_kh_config c(nolock) on kh.khid=c.khid
							left join kdcc30data..t_cl_kh_config_cash cc(nolock) on kh.khid=cc.khid
							left join
							(select khid,count(*) overcount,sum(point) overpoint from kdcc30data..t_cl_over_log olog(nolock)
							where [status]=1 and gsid=@gsid group by khid) ol on gskh.khid=ol.khid
								where gskh.gsid=@gsid and kh.wxid!='' and gskh.status=1 and kh.status=0
					open khCursor
					fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
					--判断是否回补积分
					if @profit<@r_profit*-1
						begin
							--扣除的积分设为负数
							set @msgCash=-1*@msgCash
						end
					while @@fetch_status=0
						begin
							--是否收到消息
							if not exists(select 1 from kdcc30data..t_cl_wxmess(nolock) where gsmessid=@buyMsgId and khid=@m_khid)
								begin
									fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
									continue
								end
							--如果消息需要确认，未确认的消息不发送卖出消息
							if @buyMsgConfirm=1
								begin
									if not exists (select 1 from kdcc30data..t_cl_wxmess(nolock) where gsmessid=@buyMsgId and khid=@m_khid and isnull(msgconfirm,0)=2)
										begin
											fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
											continue
										end
								end
							--如果不允许透支，则判断剩余积分是否扣除
							if @r_overcount=0
								begin
									declare @__cash int
									set @__cash=@msgCash
									if @msgCash<0
										begin
											set @__cash=-1*@__cash
										end
									--判断积分是否足够扣除，如果不够，不推送卖出
									if @m_leftcash<@__cash
										begin
											fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
											continue
										end
								end
							--如果透支次数超最大透支次数，则不再发送消息
							if @msgCash>0 and @r_overcount>0 and @m_leftcash<@msgCash and @m_overcount+1>@r_overcount
								begin
									fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
									continue
								end
							--如果透支积分大于最大透支分数，则不再发送消息
							if @msgCash>0 and @r_overpoint>0 and @m_overpoint>@r_overpoint
								begin
									fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
									continue
								end
							--最终扣除的赠送积分
							declare @md_valuecash int
							--最终扣除的现金积分
							declare @md_cash int
							set @md_valuecash=0
							set @md_cash=0
							--判断现金积分
							if @msgCash>0 and @m_cash>=@msgCash
								begin
									--赠送积分扣除0，现金积分扣除
									set @md_valuecash=0
									set @md_cash=@msgCash
									--扣除现金积分
									update kdcc30data..t_cl_kh_config_cash set leftcash=leftcash-@msgCash where khid=@m_khid
								end
							--如果现金积分不足够，分别扣除赠送积分和现金积分
							if @msgCash>0 and @m_cash>0 and @m_cash<@msgCash
								begin
									set @md_cash=@m_cash
									set @md_valuecash=@msgCash-@m_cash
									if @m_leftcash-@m_cash<@md_valuecash
										begin
											set @md_valuecash=@m_leftcash-@m_cash
										end
									--现金积分清0
									update kdcc30data..t_cl_kh_config_cash set leftcash=0 where khid=@m_khid
								end
							--如果未进入现金积分扣除逻辑，进入总积分账户逻辑判断
							if @msgCash>0 and @md_valuecash=0 and @md_cash=0
								begin
									if @m_leftcash>@msgCash
										set @md_valuecash=@msgCash
									else
										set @md_valuecash=@m_leftcash
									if @md_valuecash<0
										set @md_valuecash=0
									set @md_cash=0
								end
							--如果剩余积分<扣除的积分，透支标识+1
							if @msgCash>0
								begin
									if @m_leftcash<@msgCash
										begin
											--添加透支日志
											declare @over_cash int
											if @m_leftcash<0
												set @over_cash=@msgCash
											else
												set @over_cash=@msgCash-@m_leftcash
											insert into kdcc30data..t_cl_over_log (gsid,khid,[sid],createtime,point,updatetime,[status])
											values(@gsid,@m_khid,@id,getdate(),@over_cash,getdate(),1)
										end
									--更新积分
									update kdcc30data..t_cl_kh_config set leftcash=leftcash-@msgCash where khid=@m_khid
								end
							--如果是回补积分
							if @profit<@r_profit*-1
								begin
									--积分对冲
									exec kdcc30proc..[up_mid_98010121] @khid=@m_khid,@valuecash=@msgCash,@cashType=4,@lsh=@id,@bz='卖出回补积分'
								end
							--生成订阅消息
							insert into kdcc30data..t_cl_wxmess (gsid,gsmessid,profit,gsname,jswxid,khid,stockcode,stockname,
															stocknumber,stockprice,datatype,ordertype,tradetime,content,createdate,buytype,valuecash,cash,cashtype,dateint,position,positiondays)
															select gsid,id,profit,gsname,@m_wxid,@m_khid,stockcode,stockname,
															stocknumber,stockprice,datatype,ordertype,tradetime,content,getdate(),@m_subtype,@md_valuecash,@md_cash,1,dateint,@position,@positionDays
															from kdcc30data..t_cl_gsmess where id=@gsmsgid
							--将消费明细写往新的统一积分流水表 zhangyuchun 2015-12-28
							--只有有消费的时候才记录消费流水 zhangyuchun 2015-01-07
							if @md_cash>0 or @md_valuecash>0
								begin
								insert into kdcc30data..t_cl_points_balance_detail (itemid,customer,paytype,cash_value,give_value,status,
	createtime,reference_id) values(8,@m_khid,1,@md_cash,@md_valuecash,1,getdate(),@@identity)
								end
							--进入下一个客户判断
							fetch next from khCursor into @m_khid,@m_wxid,@m_khtype,@m_subtype,@m_leftcash,@m_overcount,@m_overpoint,@m_cash
						end
					update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),msgendtime=getdate(),msgcount=(select count(*) from kdcc30data..t_cl_wxmess(nolock) where gsmessid=@gsmsgid),step=7 where [sid]=@id
					close khCursor
					deallocate khCursor
					--回补积分的逻辑放到积分对冲的存储过程中
					--更新回补标识
					if @profit<@r_profit*-1
						begin
							update kdcc30data..t_cl_stock_log set msgtype=1 where [sid]=@id
						end
					update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),msgendtime=getdate(),msgcount=@@rowcount,step=7 where [sid]=@id
					commit tran
					update kdcc30data..t_cl_stock_log set msgcount=(select count(*) from kdcc30data..t_cl_wxmess(nolock) where gsmessid=@gsmsgid) where [sid]=@id
				end try
				begin catch
					if @@TRANCOUNT>0
					begin
						rollback tran
					end
					--记录异常
					update kdcc30data..t_cl_stock_log set updateid=@p_gybh,updatetime=getdate(),khmsg=0,msg=ERROR_MESSAGE() where [sid]=@id
					select errorcode=-1,errormsg='生成消息出现异常'
					select errorcode=-1,errormsg='生成消息出现异常'
					return
				end catch
		end
	end
------------------------------消息发送逻辑处理(step=7)-----------------------------
select @step=step from kdcc30data..t_cl_stock_log where [sid]=@id
if @step=7
	begin
		--检查是否有需要发送的消息
		if not exists (select 1 from kdcc30data..t_cl_wxmess where gsmessid=@gsmsgid)
			begin
				--更新审核日志
				update kdcc30data..t_cl_stock_log set sendstarttime=getdate(),sendendtime=getdate(),step=8 where [sid]=@id
				select errorcode=0,errormsg='审核成功'
				--返回空记录集
				select gsmessid,jswxid from kdcc30data..t_cl_wxmess (nolock) where 1=2
				return
			end
		declare @msgendtime varchar(30)
		declare @sendstarttime varchar(30)
		declare @sendendtime varchar(30)
		declare @sendmsg int
		select @sendmsg=isnull(sendmsg,0),@msgendtime=msgendtime,@sendstarttime=sendstarttime,@sendendtime=sendendtime from kdcc30data..t_cl_stock_log where [sid]=@id
		--如果距上次发送时间超过1分钟，重置发送状态
		if datediff(s,convert(datetime,@sendstarttime),getdate())>=60
			begin
				set @sendmsg=0
			end
		if @sendmsg=1
			begin
				select errorcode=-1,errormsg='消息正在发送中，不能重复发送消息'
				select errorcode=-1,errormsg='消息正在发送中，不能重复发送消息'
				return
			end
		--更新日志
		update kdcc30data..t_cl_stock_log set sendstarttime=getdate(),sendmsg=1 where [sid]=@id
		select errorcode=0,errormsg='查询成功'
		--查询数据 ###### start ###### modify by zhoujia
		--包括百度云推送需要的baiduid和devicetype
		select gsmessid,
			   jswxid,
			   wxmess.khid as khid,
			   isnull(temp.baiduid,'') as baiduid,
			   cast(isnull(temp.devicetype,0) as int) as devicetype,
			   kh.status
		from kdcc30data..t_cl_wxmess as wxmess (nolock)
		  join kdcc30data..t_cl_kh as kh
		    on kh.khid = wxmess.khid
		  left join (--找出最后一次正在使用的设备
					 select devices.khid,
					   devices.devicetype,
					   devices.baiduid,
					   devices.bindtime,
					   devices.lasttime,
					   devices.bused
					 from kdcc30data..t_cl_devices as devices (nolock)
					 where devices.bused=1 --正在使用的设备
					 ) as temp
			on wxmess.khid = temp.khid
		where gsmessid=@gsmsgid
		--查询数据 ######  end  ###### modify by zhoujia
		update kdcc30data..t_cl_stock_log set readendtime=getdate() where [sid]=@id
	end
end
GO