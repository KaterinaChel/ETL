create or replace procedure dm.fill_f101_round_f ( 
  i_OnDate  date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
    call dm.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call dm.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );

    delete
      from dm.DM_F101_ROUND_F f
     where from_date = date_trunc('month', i_OnDate)  
       and to_date = (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day');
   
    call dm.writelog('insert', 1);
   
    insert 
      into dm.dm_f101_round_f
           ( from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub      
           , turn_cre_val      
           , turn_cre_total    
           , balance_out_rub  
           , balance_out_val   
           , balance_out_total
           , r_balance_in_rub
           , r_balance_in_val
           , r_balance_in_total
           , r_turn_deb_rub
           , r_turn_deb_val
           , r_turn_deb_total
           , r_turn_cre_rub
           , r_turn_cre_val
           , r_turn_cre_total
           , r_balance_out_rub
           , r_balance_out_val
           , r_balance_out_total 
           )
    select  date_trunc('month', i_OnDate)        as from_date,
           (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')  as to_date,
           s.chapter                             as chapter,
           substr(acc_d.account_number, 1, 5)    as ledger_account,
           acc_d.char_type                       as characteristic,
           -- RUB balance
           sum( case 
                  when cur.currency_code in ('643', '810')
                  then coalesce(b.balance_out,0)
                  else 0
                 end
              )                                 as balance_in_rub,
          -- VAL balance converted to rub
          sum( case 
                 when cur.currency_code not in ('643', '810')
                 then coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0)
                 else 0
                end
             )                                   as balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          sum(  case 
                 when cur.currency_code in ('643', '810')
                 then coalesce(b.balance_out,0)
                 else coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0)
               end
             )                                 as balance_in_total  ,
           -- RUB debet turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then coalesce(at.debet_amount_rub,0)
                 else 0
               end
           )                                     as turn_deb_rub,
           -- VAL debet turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then coalesce(at.debet_amount_rub,0)
                 else 0
               end
           )                                     as turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
           sum(coalesce(at.debet_amount_rub,0))             as turn_deb_total,
           -- RUB credit turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then coalesce(at.credit_amount_rub,0)
                 else 0
               end
              )                                 as turn_cre_rub,
           -- VAL credit turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then coalesce(at.credit_amount_rub,0)
                 else 0
               end
              )                               as turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           sum(coalesce(at.credit_amount_rub,0))            as turn_cre_total,
           -- BALANCE_OUT_RUB 

           coalesce(sum(case 
                 when cur.currency_code ='643' and acc_d.char_type = 'A'
                 then coalesce(b.balance_out,0) - coalesce(at.credit_amount_rub,0) + coalesce(at.debet_amount_rub,0)
                 when cur.currency_code ='810' and acc_d.char_type = 'A'
                 then coalesce(b.balance_out,0) - coalesce(at.credit_amount_rub,0) + coalesce(at.debet_amount_rub,0) 
                 when cur.currency_code ='643' and acc_d.char_type = 'P'
                 then coalesce(b.balance_out,0) + coalesce(at.credit_amount_rub,0) - coalesce(at.debet_amount_rub,0)
                 when cur.currency_code ='810' and acc_d.char_type = 'P'
                 then coalesce(b.balance_out,0) + coalesce(at.credit_amount_rub,0) - coalesce(at.debet_amount_rub,0)
                 else 0
               end
              ),0)                                  as balance_out_rub,
           -- BALANCE_OUT_VAL    
           coalesce(sum(case 
                 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'A'
                 then coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0) - coalesce(at.credit_amount_rub,0)  + coalesce(at.debet_amount_rub,0)
                 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'P'
                 then coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0) + coalesce(at.credit_amount_rub,0)  - coalesce(at.debet_amount_rub,0)
                 else 0
               end
              ),0)                                  as balance_out_val,
           -- BALANCE_OUT_TOTAL как BALANCE_OUT_VAL + BALANCE_OUT_RUB   
           coalesce(sum(case 
                 when cur.currency_code in ('643', '810') and acc_d.char_type = 'A'
                 then coalesce(b.balance_out,0) - coalesce(at.credit_amount_rub,0) + coalesce(at.debet_amount_rub,0)
                 when cur.currency_code in ('643', '810') and acc_d.char_type = 'P'
                 then coalesce(b.balance_out,0) + coalesce(at.credit_amount_rub,0) - coalesce(at.debet_amount_rub,0)
                 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'A'
                 then coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0) - coalesce(at.credit_amount_rub,0)  + coalesce(at.debet_amount_rub,0)
                 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'P'
                 then coalesce(b.balance_out * coalesce(exch_r.reduced_cource,1),0) + coalesce(at.credit_amount_rub,0)  - coalesce(at.debet_amount_rub,0)
                 else 0
               end
              ),0)                               as balance_out_total,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_in_rub,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_in_val,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_in_total,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_deb_rub,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_deb_val,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_deb_total,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_cre_rub,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_cre_val,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_turn_cre_total,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_out_rub,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_out_val,
              COALESCE(CAST(NULL AS NUMERIC), 0) as r_balance_out_total

      from ds.md_ledger_account_s s
      join ds.md_account_d acc_d
        on substr(acc_d.account_number, 1, 5) = to_char(s.ledger_account, 'FM99999999')
      join ds.md_currency_d cur
        on cur.currency_rk = acc_d.currency_rk
      left 
      join ds.ft_balance_f b
        on b.account_rk = acc_d.account_rk
       and b.on_date  = (date_trunc('month', i_OnDate) - INTERVAL '1 day')
      left 
      join ds.md_exchange_rate_d exch_r
        on exch_r.currency_rk = acc_d.currency_rk
       and i_OnDate between exch_r.data_actual_date and exch_r.data_actual_end_date
      left 
      join dm.dm_account_turnover_f at
        on at.account_rk = acc_d.account_rk
       and at.on_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
     where i_OnDate between s.start_date and s.end_date
       and i_OnDate between acc_d.data_actual_date and acc_d.data_actual_end_date
       and i_OnDate between cur.data_actual_date and cur.data_actual_end_date
     group by s.chapter,
           substr(acc_d.account_number, 1, 5),
           acc_d.char_type;
	
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call dm.writelog('[END] inserted ' ||  to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    commit;
    
  end;$$


