/*
*author:zhangyuchun
*description:积分对冲
*adddate:2015-07-07
*editdate:2015-07-13  添加积分获取明细日志记录
		  2015-10-14  修改更新现金积分表的条件
		  2015-12-28  获取明细从t_cl_kh_intocash更改到t_cl_points_balance_detail
		  2016-01-20  获取明细添加对非注册用户的支持
*/
ALTER PROCEDURE [dbo].[up_mid_98010121]
	@openid    VARCHAR(50) = '', --客户微信标识
	@khid      INT = 0, --客户id
	@valuecash INT = 0, --赠送积分
	@cash      INT = 0, --充值积分
	@cashType  INT = 0, --积分获取类型
	@lsh       INT = 0, --流水号
	@bz        VARCHAR(100) --备注
AS
BEGIN
	IF @valuecash < 0
		SET @valuecash = -1 * @valuecash
	DECLARE @intoid INT
	--添加获取明细
	INSERT INTO kdcc30data..t_cl_kh_intocash (khid, cash, cashtype, lsh, bz, dateint, createtime, cashprice)
	VALUES (@khid, @valuecash + @cash, @cashType, @lsh, @bz, convert(VARCHAR(8), getDate(), 112), getDate(),
					@cash)
	--根据bz查找对应的积分项
	SELECT @cashType = id
	FROM kdcc30data..t_cl_points_balance_item
	WHERE name = @bz
	--获取明细从t_cl_kh_intocash更改到t_cl_points_balance_detail
	--获取明细有可能是非注册用户，非注册用户插入流水，直接返回 zhangyuchun 2016-01-20
	INSERT INTO kdcc30data..t_cl_points_balance_detail
	(itemid, customer, paytype, cash_value, give_value, status, createtime, reference_id, remark)
	VALUES (@cashType, isnull(@khid, 0), 1, @cash, @valuecash, 1, getdate(), @lsh, @openid)
	SELECT @intoid = @@identity
	IF isnull(@khid, 0) = 0 AND isnull(@openid, '') != ''
		RETURN
		--更新账户积分信息
		update kdcc30data..t_cl_kh_config SET totalcash = totalcash + @valuecash + @cash, leftcash = leftcash + @valuecash +
																																																 @cash, cashprice =
isnull(cashprice, 0) + @cash WHERE khid = @khid
	--如果没有现金积分，建立新的现金积分账户
	IF NOT exists(SELECT 1
								FROM kdcc30data..t_cl_kh_config_cash
								WHERE khid = @khid) AND @cash > 0
		BEGIN
			INSERT INTO kdcc30data..t_cl_kh_config_cash (khid, leftcash, totalcash, createtime, cashprice, totalpoint)
			VALUES (@khid, 0, 0, getdate(), 0, 0)
		END
	--如果金额大于0，需要更新现金积分表
	IF @cash > 0
		BEGIN
			UPDATE kdcc30data..t_cl_kh_config_cash
			SET totalcash = totalcash + @cash, cashprice = isnull(cashprice, 0) + @cash,
				totalpoint  = isnull(totalpoint, 0) + @valuecash + @cash
			WHERE khid = @khid
		END
	--如果没有透支，直接返回，不需要作任何处理
	IF NOT exists(SELECT 1
								FROM kdcc30data..t_cl_over_log
								WHERE khid = @khid AND [status] = 1)
		BEGIN
			UPDATE kdcc30data..t_cl_kh_config_cash
			SET leftcash = leftcash + @cash
			WHERE khid = @khid
			RETURN
		END
	--变量定义
	DECLARE @t_totalpoint INT
	SELECT @t_totalpoint = (sum(point) - isnull(sum(pointed), 0))
	FROM (
				 SELECT
					 l.id,
					 point,
					 pointed
				 FROM kdcc30data..t_cl_over_log l
					 LEFT JOIN (SELECT
												oid,
												sum(valuecash) + sum(cash) pointed
											FROM kdcc30data..t_cl_over_detail
											GROUP BY oid) d ON l.id = d.oid
				 WHERE khid = @khid AND [status] = 1) t
	IF @t_totalpoint IS NULL OR @t_totalpoint = 0
		BEGIN
			UPDATE kdcc30data..t_cl_kh_config_cash
			SET leftcash = leftcash + @cash
			WHERE khid = @khid
			RETURN
		END
	--计算余数
	DECLARE @y_valuecash INT
	DECLARE @y_cash INT
	SELECT
		@y_valuecash = @valuecash - isnull(sum(valuecash), 0),
		@y_cash = @cash - isnull(sum(cash), 0)
	FROM (
				 SELECT
					 convert(INT, convert(NUMERIC(19, 4), point) / @t_totalpoint * @valuecash) valuecash,
					 convert(INT, convert(NUMERIC(19, 4), point) / @t_totalpoint * @cash)      cash
				 FROM (
								SELECT (l.point - isnull(pointed, 0)) point
								FROM kdcc30data..t_cl_over_log l INNER JOIN kdcc30data..t_cl_gs_stock s
										ON l.[sid] = s.id
									INNER JOIN kdcc30data..t_cl_wxmess m ON s.gsid = m.gsid AND
																													s.stockcode = m.stockcode AND s.stockname = m.stockname AND
																													s.shuliang = m.stocknumber
																													AND s.price = m.stockprice AND s.jydatetime = m.tradetime
									LEFT JOIN (SELECT
															 oid,
															 sum(valuecash) + sum(cash) pointed
														 FROM kdcc30data..t_cl_over_detail
														 GROUP BY oid) d ON l.id = d.oid
								WHERE l.khid = @khid AND m.khid = @khid AND l.[status] = 1) t) tt
	--透支明细处理
	--透支明细变量声明
	DECLARE @over_id INT
	DECLARE @over_point INT
	DECLARE @over_msgid INT
	DECLARE @over_valuecash INT
	DECLARE @over_cash INT

	DECLARE @i INT --iterator
	DECLARE @iRwCnt INT --rowcount

	CREATE TABLE #tmp_2 (
		id        INT IDENTITY (1, 1),
		overid    INT NULL,
		point     INT NULL,
		msgid     INT NULL,
		valuecash INT NULL,
		cash      INT NULL
	)

	INSERT INTO #tmp_2
		SELECT
			l.id                           overid,
			(l.point - isnull(pointed, 0)) point,
			m.id                           msgid,
			m.valuecash,
			m.cash
		FROM kdcc30data..t_cl_over_log l INNER JOIN kdcc30data..t_cl_gs_stock s
				ON l.[sid] = s.id
			INNER JOIN kdcc30data..t_cl_wxmess m ON s.gsid = m.gsid AND
																							s.stockcode = m.stockcode AND s.stockname = m.stockname AND
																							s.shuliang = m.stocknumber
																							AND s.price = m.stockprice AND s.jydatetime = m.tradetime
			LEFT JOIN (SELECT
									 oid,
									 sum(valuecash) + sum(cash) pointed
								 FROM kdcc30data..t_cl_over_detail
								 GROUP BY oid) d ON l.id = d.oid
		WHERE l.khid = @khid AND m.khid = @khid AND l.[status] = 1
		ORDER BY l.createtime ASC


	SET @iRwCnt = @@ROWCOUNT --SCOPE_IDENTITY() would also work

  CREATE CLUSTERED INDEX idx_tmp ON #tmp_2(id) WITH FILLFACTOR = 100

	DECLARE @ed_valuecash INT
	SET @ed_valuecash = 0
	DECLARE @ed_cash INT
	SET @ed_cash = 0
	DECLARE @index INT
	SET @index = 0

	WHILE @i <= @iRwCnt
		BEGIN

			SELECT @over_id = overid, @over_point = point, @over_msgid = msgid, @over_cash = cash, @over_valuecash = valuecash
			FROM #tmp_2
			WHERE id = @i

			DECLARE @currValueCash INT
			--按比例计算可以对冲的充值积分数
			DECLARE @currCash INT
			SET @currValueCash = convert(INT, convert(NUMERIC(19, 4), @over_point) / @t_totalpoint * @valuecash)
			SET @currCash = convert(INT, convert(NUMERIC(19, 4), @over_point) / @t_totalpoint * @cash)
			IF @index = 0
				BEGIN
					BEGIN
						SET @currValueCash = @currValueCash + @y_valuecash
						SET @currCash = @currCash + @y_cash
					END
				END
			SET @index = @index + 1
			IF @currValueCash = 0 AND @currCash = 0
				BEGIN
					SET @i = @i + 1
					CONTINUE
				END
			--如果完全对冲
			IF (@currValueCash + @currCash) >= @over_point
				BEGIN
					--清除透支标识
					UPDATE kdcc30data..t_cl_over_log
					SET [status] = 0
					WHERE id = @over_id
					--如果现金积分足够对冲，先对冲现金积分
					IF @currCash >= @over_point
						BEGIN
							--消费的现金积分+@over_point
							--update kdcc30data..t_cl_wxmess set cash=cash+@over_point where id=@over_msgid
							--添加对冲明细
							INSERT INTO kdcc30data..t_cl_over_detail (oid, iid, valuecash, cash, createtime)
							VALUES (@over_id, @intoid, 0, @over_point, getdate())
							--更新对冲状态
							UPDATE kdcc30data..t_cl_over_log
							SET [status] = 0
							WHERE id = @over_id
							--保存已经对冲的现金积分
							SET @ed_cash = @ed_cash + @over_point
							SET @i = @i + 1
							CONTINUE
						END
					--如果现金积分为0，赠送积分足够对冲
					IF @currCash = 0 AND @currValueCash >= @over_point
						BEGIN
							--消费的现金积分+@over_point
							--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@over_point where id=@over_msgid
							--添加对冲明细
							INSERT INTO kdcc30data..t_cl_over_detail (oid, iid, valuecash, cash, createtime)
							VALUES (@over_id, @intoid, @over_point, 0, getdate())
							--更新对冲状态
							UPDATE kdcc30data..t_cl_over_log
							SET [status] = 0
							WHERE id = @over_id
							--保存已经对冲的现金积分
							SET @ed_valuecash = @ed_valuecash + @over_point
							SET @i = @i + 1
							CONTINUE
						END
					--如果需要分别对冲
					IF @currCash < @over_point AND @currValueCash < @over_point
						BEGIN
							--分别对冲
							DECLARE @_cash INT
							DECLARE @_valuecash INT
							SET @_cash = @currCash
							SET @_valuecash = @over_point - @_cash
							--添加对冲明细
							INSERT INTO kdcc30data..t_cl_over_detail (oid, iid, valuecash, cash, createtime)
							VALUES (@over_id, @intoid, @_valuecash, @_cash, getdate())
							--更新对冲状态
							UPDATE kdcc30data..t_cl_over_log
							SET [status] = 0
							WHERE id = @over_id
							--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@_valuecash,cash=cash+@cash where id=@over_msgid
							SET @ed_valuecash = @ed_valuecash + @_valuecash
							SET @ed_cash = @ed_cash + @_cash
						END
				END
			--如果不能全部对冲
			ELSE
				BEGIN
					--update kdcc30data..t_cl_wxmess set valuecash=valuecash+@currValueCash,cash=cash+@currCash where id=@over_msgid
					--添加对冲明细
					INSERT INTO kdcc30data..t_cl_over_detail (oid, iid, valuecash, cash, createtime)
					VALUES (@over_id, @intoid, @currValueCash, @currCash, getdate())
					SET @ed_valuecash = @ed_valuecash + @currValueCash
					SET @ed_cash = @ed_cash + @currCash
				END



		SET @i = @i + 1
    END
  DROP TABLE #tmp_2
  
	--剩余的赠送积分和剩余的现金积分
	--剩余的赠送积分=@valuecash-@ed_valuecash
	--剩余的现金积分=@cash-@ed_cash
	--更新客户积分账户
	--更新客户现金积分账户
	IF @cash - @ed_cash > 0
		BEGIN
			UPDATE kdcc30data..t_cl_kh_config_cash
			SET leftcash = leftcash + (@cash - @ed_cash)
			WHERE khid = @khid
		END
END
GO
