/*
*author:zhangyuchun
*description:积分对冲
*adddate:2015-07-07
*editdate:2015-07-13  添加积分获取明细日志记录
		  2015-10-14  修改更新现金积分表的条件
		  2015-12-28  获取明细从t_cl_kh_intocash更改到t_cl_points_balance_detail
		  2016-01-20  获取明细添加对非注册用户的支持
*/
ALTER procedure [dbo].[up_mid_98010121]
@openid varchar(50)='', --客户微信标识
@khid int=0, --客户id
@valuecash int=0, --赠送积分
@cash int=0, --充值积分
@cashType int=0, --积分获取类型
@lsh int=0, --流水号
@bz varchar(100) --备注
as
begin
	if @valuecash<0
		set @valuecash=-1*@valuecash
	declare @intoid int
	--添加获取明细
	insert into kdcc30data..t_cl_kh_intocash (khid,cash,cashtype,lsh,bz,dateint,createtime,cashprice)
				values (@khid,@valuecash+@cash,@cashType,@lsh,@bz,convert(varchar(8),getDate(),112),getDate(),
				@cash)
	--根据bz查找对应的积分项
	select @cashType=id from kdcc30data..t_cl_points_balance_item where name=@bz
	--获取明细从t_cl_kh_intocash更改到t_cl_points_balance_detail
	--获取明细有可能是非注册用户，非注册用户插入流水，直接返回 zhangyuchun 2016-01-20
	insert into kdcc30data..t_cl_points_balance_detail
		(itemid,customer,paytype,cash_value,give_value,status,createtime,reference_id,remark)
	values(@cashType,isnull(@khid,0),1,@cash,@valuecash,1,getdate(),@lsh,@openid)
	select @intoid=@@identity
	if isnull(@khid,0)=0 and isnull(@openid,'')!=''
		return
	--更新账户积分信息
	update kdcc30data..t_cl_kh_config set totalcash=totalcash+@valuecash+@cash,leftcash=leftcash+@valuecash+@cash,cashprice=isnull(cashprice,0)+@cash where khid=@khid
	--如果没有现金积分，建立新的现金积分账户
	if not exists(select 1 from kdcc30data..t_cl_kh_config_cash where khid=@khid) and @cash>0
		begin
			insert into kdcc30data..t_cl_kh_config_cash (khid,leftcash,totalcash,createtime,cashprice,totalpoint)
							values(@khid,0,0,getdate(),0,0)
		end
	--如果金额大于0，需要更新现金积分表
	if @cash>0
		begin
			update kdcc30data..t_cl_kh_config_cash set totalcash=totalcash+@cash,cashprice=isnull(cashprice,0)+@cash,totalpoint=isnull(totalpoint,0)+@valuecash+@cash
			where khid=@khid
		end
	--如果没有透支，直接返回，不需要作任何处理
	if not exists(select 1 from kdcc30data..t_cl_over_log where khid=@khid and [status]=1)
		begin
			update kdcc30data..t_cl_kh_config_cash set leftcash=leftcash+@cash where khid=@khid
			return
		end
	--变量定义
	declare @t_totalpoint int
	select @t_totalpoint=(sum(point)-isnull(sum(pointed),0)) from (
select l.id,point,pointed from kdcc30data..t_cl_over_log l
left join (select oid,sum(valuecash)+sum(cash) pointed from kdcc30data..t_cl_over_detail
group by oid) d on l.id=d.oid where khid=@khid and [status]=1) t
	if @t_totalpoint is null or @t_totalpoint=0
		begin
			update kdcc30data..t_cl_kh_config_cash set leftcash=leftcash+@cash where khid=@khid
			return
		end
	--计算余数
	declare @y_valuecash int
	declare @y_cash int
	select @y_valuecash=@valuecash-isnull(sum(valuecash),0),@y_cash=@cash-isnull(sum(cash),0) from (
	select convert(int,convert(numeric(19,4),point)/@t_totalpoint*@valuecash) valuecash
	,convert(int,convert(numeric(19,4),point)/@t_totalpoint*@cash) cash
	from (
	select (l.point-isnull(pointed,0)) point from kdcc30data..t_cl_over_log l inner join kdcc30data..t_cl_gs_stock s
	on l.[sid]=s.id inner join kdcc30data..t_cl_wxmess m on s.gsid=m.gsid and
	s.stockcode=m.stockcode and s.stockname=m.stockname and s.shuliang=m.stocknumber
	and s.price=m.stockprice and s.jydatetime=m.tradetime
	left join (select oid,sum(valuecash)+sum(cash) pointed from kdcc30data..t_cl_over_detail
	group by oid) d on l.id=d.oid
	where l.khid=@khid and m.khid=@khid and l.[status]=1)t ) tt
	--透支明细处理
	--透支明细变量声明
	declare @over_id int
	declare @over_point int
	declare @over_msgid int
	declare @over_valuecash int
	declare @over_cash int
	declare point_cursor cursor for select l.id overid,(l.point-isnull(pointed,0)) point,m.id msgid,m.valuecash,m.cash from kdcc30data..t_cl_over_log l inner join kdcc30data..t_cl_gs_stock s
on l.[sid]=s.id inner join kdcc30data..t_cl_wxmess m on s.gsid=m.gsid and
s.stockcode=m.stockcode and s.stockname=m.stockname and s.shuliang=m.stocknumber
and s.price=m.stockprice and s.jydatetime=m.tradetime
left join (select oid,sum(valuecash)+sum(cash) pointed from kdcc30data..t_cl_over_detail
group by oid) d on l.id=d.oid
where l.khid=@khid and m.khid=@khid and l.[status]=1 order by l.createtime asc
	open point_cursor
	fetch next from point_cursor into @over_id,@over_point,@over_msgid,@over_valuecash,@over_cash
	--保存一共对冲的积分数
	declare @ed_valuecash int
	set @ed_valuecash=0
	declare @ed_cash int
	set @ed_cash=0
	declare @index int
	set @index=0
	while @@fetch_status=0
		begin
			--按比例计算可以对冲的赠送积分数
			declare @currValueCash int
			--按比例计算可以对冲的充值积分数
			declare @currCash int
			set @currValueCash=convert(int,convert(numeric(19,4),@over_point)/@t_totalpoint*@valuecash)
			set @currCash=convert(int,convert(numeric(19,4),@over_point)/@t_totalpoint*@cash)
			if @index=0
				begin
						begin
							set @currValueCash=@currValueCash+@y_valuecash
							set @currCash=@currCash+@y_cash
						end
				end
			set @index=@index+1
			if @currValueCash=0 and @currCash=0
				begin
					fetch next from point_cursor into @over_id,@over_point,@over_msgid,@over_valuecash,@over_cash
					continue
				end
			--如果完全对冲
			if (@currValueCash+@currCash)>=@over_point
					begin
					--清除透支标识
					update kdcc30data..t_cl_over_log set [status]=0 where id=@over_id
					--如果现金积分足够对冲，先对冲现金积分
					if @currCash>=@over_point
						begin
							--消费的现金积分+@over_point
							--update kdcc30data..t_cl_wxmess set cash=cash+@over_point where id=@over_msgid
							--添加对冲明细
							insert into kdcc30data..t_cl_over_detail (oid,iid,valuecash,cash,createtime)
							values(@over_id,@intoid,0,@over_point,getdate())
							--更新对冲状态
							update kdcc30data..t_cl_over_log set [status]=0 where id=@over_id
							--保存已经对冲的现金积分
							set @ed_cash=@ed_cash+@over_point
							fetch next from point_cursor into @over_id,@over_point,@over_msgid,@over_valuecash,@over_cash
							continue
						end
					--如果现金积分为0，赠送积分足够对冲
					if @currCash=0 and @currValueCash>=@over_point
						begin
							--消费的现金积分+@over_point
							--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@over_point where id=@over_msgid
							--添加对冲明细
							insert into kdcc30data..t_cl_over_detail (oid,iid,valuecash,cash,createtime)
							values(@over_id,@intoid,@over_point,0,getdate())
							--更新对冲状态
							update kdcc30data..t_cl_over_log set [status]=0 where id=@over_id
							--保存已经对冲的现金积分
							set @ed_valuecash=@ed_valuecash+@over_point
							fetch next from point_cursor into @over_id,@over_point,@over_msgid,@over_valuecash,@over_cash
							continue
						end
					--如果需要分别对冲
					if @currCash<@over_point and @currValueCash<@over_point
						begin
							--分别对冲
							declare @_cash int
							declare @_valuecash int
							set @_cash=@currCash
							set @_valuecash=@over_point-@_cash
							--添加对冲明细
							insert into kdcc30data..t_cl_over_detail (oid,iid,valuecash,cash,createtime)
							values(@over_id,@intoid,@_valuecash,@_cash,getdate())
							--更新对冲状态
							update kdcc30data..t_cl_over_log set [status]=0 where id=@over_id
							--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@_valuecash,cash=cash+@cash where id=@over_msgid
							set @ed_valuecash=@ed_valuecash+@_valuecash
							set @ed_cash=@ed_cash+@_cash
						end
				end
			--如果不能全部对冲
			else
				begin
					--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@currValueCash,cash=cash+@currCash where id=@over_msgid
					--添加对冲明细
					insert into kdcc30data..t_cl_over_detail (oid,iid,valuecash,cash,createtime) values(@over_id,@intoid,@currValueCash,@currCash,getdate())
					set @ed_valuecash=@ed_valuecash+@currValueCash
					set @ed_cash=@ed_cash+@currCash
				end
			--下一次循环
			fetch next from point_cursor into @over_id,@over_point,@over_msgid,@over_valuecash,@over_cash
		end
		close point_cursor
		deallocate point_cursor
		--剩余的赠送积分和剩余的现金积分
		--剩余的赠送积分=@valuecash-@ed_valuecash
		--剩余的现金积分=@cash-@ed_cash
		--更新客户积分账户
		--更新客户现金积分账户
		if @cash-@ed_cash>0
			begin
				update kdcc30data..t_cl_kh_config_cash set leftcash=leftcash+(@cash-@ed_cash) where khid=@khid
			end
end
GO