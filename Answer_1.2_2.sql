/*

delete from DM.DM_ACCOUNT_BALANCE_F;

select * from DM.DM_ACCOUNT_BALANCE_F;


DO $$

declare 
my_date date := '01-01-2018'; 
begin

insert into logs.logs_ds
(etl_table, date_start, operation_status)
values ('DM_ACCOUNT_BALANCE_F',clock_timestamp()::TIME,8);


for counter in 1..31 loop
call ds.fill_account_balance_f(my_date);
raise notice '%' ,my_date;
my_date = my_date + INTERVAL '1 day';

end loop;

UPDATE logs.logs_ds 
SET
DATE_END = NOW()::TIME,
OPERATION_STATUS = 0,
TIME_ETL = clock_timestamp()::TIME - DATE_START
WHERE
OPERATION_STATUS = 8;
end $$ LANGUAGE PLPGSQL;
*/
INSERT INTO
	DM.DM_ACCOUNT_BALANCE_F
SELECT DISTINCT
	FT."ON_DATE",
	FT."ACCOUNT_RK",
	FT."BALANCE_OUT",
	COALESCE(
		FT."BALANCE_OUT" * EXC."REDUCED_COURCE",
		FT."BALANCE_OUT" * 1
	)
FROM
	DS.FT_BALANCE_F AS FT
	LEFT JOIN DS.MD_EXCHANGE_RATE_D AS EXC ON FT."CURRENCY_RK" = EXC."CURRENCY_RK"
	AND '2017-12-31' BETWEEN EXC."DATA_ACTUAL_DATE" AND EXC."DATA_ACTUAL_END_DATE"
WHERE
	FT."ON_DATE" = '2017-12-31';

--delete from DM.DM_ACCOUNT_BALANCE_F;
CREATE
OR REPLACE PROCEDURE DS.FILL_ACCOUNT_BALANCE_F (I_ONDATE DATE) AS $$
begin
insert into DM.DM_ACCOUNT_BALANCE_F
SELECT distinct i_ondate,
	acc_b.ACCOUNT_RK,
	case when acc."CHAR_TYPE" ='П' then coalesce(acc_b_ld.BALANCE_OUT,0) +coalesce(acc_tur.credit_amount,0)- coalesce(acc_tur.debet_amount,0)  
	when acc."CHAR_TYPE" ='А' then coalesce(acc_b_ld.BALANCE_OUT,0) + coalesce(acc_tur.debet_amount,0) -  coalesce(acc_tur.credit_amount,0)
	else acc_b_ld.BALANCE_OUT
	end as new_balance,
	case when acc."CHAR_TYPE" ='П' then  coalesce((coalesce(acc_b_ld.BALANCE_OUT,0) +coalesce(acc_tur.credit_amount,0)- coalesce(acc_tur.debet_amount,0))*exc."REDUCED_COURCE",(coalesce(acc_b_ld.BALANCE_OUT,0) +coalesce(acc_tur.credit_amount,0)- coalesce(acc_tur.debet_amount,0))*1)  
	when acc."CHAR_TYPE" ='А' then coalesce((coalesce(acc_b_ld.BALANCE_OUT,0) + coalesce(acc_tur.debet_amount,0) -  coalesce(acc_tur.credit_amount,0))*exc."REDUCED_COURCE",(coalesce(acc_b_ld.BALANCE_OUT,0) + coalesce(acc_tur.debet_amount,0) -  coalesce(acc_tur.credit_amount,0))*1)
	else acc_b_ld.BALANCE_OUT_RUB
	end as new_balance_rub
	FROM DM.DM_ACCOUNT_BALANCE_F as acc_b
	join ds.md_account_d as acc
	on acc_b.ACCOUNT_RK = acc."ACCOUNT_RK" and i_ondate between acc."DATA_ACTUAL_DATE" and acc."DATA_ACTUAL_END_DATE" 
	left join ds.md_exchange_rate_d as exc on acc."CURRENCY_RK" = exc."CURRENCY_RK" and i_ondate between exc."DATA_ACTUAL_DATE" and exc."DATA_ACTUAL_END_DATE"
	left join DM.DM_ACCOUNT_BALANCE_F as  acc_b_ld on acc_b.ACCOUNT_RK = acc_b_ld.ACCOUNT_RK and acc_b_ld.ON_DATE = ( i_ondate - interval '1 day')
	left join dm.DM_ACCOUNT_TURNOVER_F as acc_tur on acc_tur.account_rk = acc_b.ACCOUNT_RK and  i_ondate = acc_tur.on_date;
		end ;
 $$ LANGUAGE PLPGSQL;