#!/usr/bin/env ruby

# frozen_string_literal: true

# To run locally: ruby sequel_export_full_season.rb 2022 dev
# bundle exec sequel_export_full_season.rb 2022 dev

# NOTE: This process uses postgresql's COPY command which needs a writeable dir for caching.
#       `nspack` in this example is the user group name.
#
#     sudo mkdir /var/lib/postgresql/workarea
#     sudo chown postgres:nspack /var/lib/postgresql/workarea/
#     sudo chmod g+w /var/lib/postgresql/workarea/

# A butchered copy of the Ruby 1.8.7 full season extract in the exporter system.
# This code only has to run for a season, so a lot of configuration has been hard-coded.
# That is also why the design is a bit peculiar...

# rubocop:disable Layout/LineLength
# rubocop:disable Naming/VariableNumber
# rubocop:disable Metrics/MethodLength
# rubocop:disable Metrics/ClassLength
# rubocop:disable Style/OptionalBooleanParameter

# Note for setup:
# ---------------
# Make the csv file's dir accessible to postgres and this app:
# chown postgres:unifrutti /var/lib/postgresql/workarea
# chmod g+w  /var/lib/postgresql/workarea

require 'bundler'
Bundler.require(:default, 'development')

# require 'dotenv'

require 'sequel'
require 'logger'

DATABASE_URL = 'postgres://postgres:postgres@localhost/exporter'

DB = Sequel.connect(DATABASE_URL)
Sequel.application_timezone = :local
Sequel.database_timezone = :utc

RAILS_DEFAULT_LOGGER = Logger.new('log/full_season_extracts.log', 10, 1_024_000)

year = ARGV[0]
raise "Year #{year} does not look right (2013 - 2023)" unless (2013..2023).include?(year.to_i)

DEV_MODE = ARGV[1] == 'dev'

# Spoof the exporter client settings to be able to use the old code as-is in many parts.
class ClientSettings
  def self.full_season_super_group_splits
    %w[BLY-SW DUN-SW,UDC-SW UBC-SW]
  end

  def self.full_season_marketing_desk_splits
    ['Unifrutti Distribution']
  end

  def self.full_seasons_out_dir
    '~/edi_out/tosend/joined'
  end

  def self.bi_upload_server
    'uf_jmt@172.16.0.21'
  end

  def self.bi_upload_path
    '/home/uf_jmt/ldg'
  end

  def self.bi_upload_bly
    false
  end

  def self.bi_upload_parallel
    nil
  end

  def self.rsync_target
    '172.16.0.17'
  end

  def self.postgres_workarea
    '/var/lib/postgresql/workarea' # Place where csv files can be written by the app and read by postgres.
  end

  def self.system_err_recipients
    'james@nosoft.biz,hans@nosoft.biz,miracle@nosoft.biz,patie@nosoft.biz'
  end

  def self.from_mail
    'Unifrutti System<jmt@unifrutti.co.za>'
  end
end

Mail.defaults do
  if DEV_MODE
    delivery_method :logger
  else
    delivery_method :smtp,
                    domain: 'unifrutti.co.za',
                    authentication: :login,
                    user_name: 'jmt',
                    password: 'un1frut@jmt',
                    address: '172.16.0.12',
                    port: '25'
  end
end

# Send mail
class Globals
  def self.send_an_email(subject, recipients, body)
    mail = Mail.new do
      from    ClientSettings.from_mail
      to      recipients
      subject subject
      body    body
    end

    mail.deliver!
    # rescue Net::SMTPAuthenticationError,
    #        Net::SMTPServerBusy,
    #        Net::SMTPSyntaxError,
    #        Net::SMTPFatalError,
    #        Net::SMTPUnknownError,
    #        Errno::ECONNREFUSED => e
    #   log_mail_fail(e)
    #   raise
  end
end

# Base class for exports
class ExportTask
  require 'csv'

  def initialize(table_name, extract_name)
    @event_log_id = nil
    @table_name   = table_name
    @extract_name = extract_name
    @send_mail    = true
    @record_count = 0
    # @inflector = Dry::Inflector.new
  end

  private

  # Helper method for logging progress with time.
  def log_step(msg, ended = false)
    RAILS_DEFAULT_LOGGER.info ">>> #{Time.now.strftime('%Y-%m-%d %H:%M:%S')} #{msg}"
    if @event_log_id.nil?
      # @event_log_id = ActiveRecord::Base.connection.select_value(<<-SQL)
      # INSERT INTO dw_extract_events(table_name, event_time, description, event_log, is_complete)
      # VALUES('#{@table_name}', localtimestamp, '#{@extract_name}', date_trunc('second', localtimestamp) || ': #{msg.gsub("'", '`')}', #{ended})
      # RETURNING id;
      # SQL
      @event_log_id = DB[:dw_extract_events].insert(table_name: @table_name,
                                                    event_time: Time.now,
                                                    description: @extract_name,
                                                    event_log: "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}: #{msg.gsub("'", '`')}",
                                                    is_complete: ended)
    else
      completed_at = if ended
                       "'#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}'"
                     else
                       'NULL'
                     end
      DB.run(<<-SQL)
      UPDATE dw_extract_events SET event_log = event_log || E'\n' || '#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}: #{msg.gsub("'", '`')}',
                                   is_complete = #{ended}, completed_at = #{completed_at}
                         WHERE id = #{@event_log_id};
      SQL
    end
  end

  # On exception, log the error and send an email.
  def handle_error(err)
    error_mail_recipients = ClientSettings.system_err_recipients
    RAILS_DEFAULT_LOGGER.info err.message
    RAILS_DEFAULT_LOGGER.info err.backtrace.join("\n")
    s = if err.message == "nil can't be coerced into BigDecimal"
          <<-STR
            If an invoice has an ETD > 5 days ahead, a division by NULL usd_etd_roe will raise an exception.

            To find the invoice(s) in error, run the following SQL:

              SELECT invoices.id, invoices.invoice_ref_no,
              invoices.estimate_departure_date, invoices.actual_departure_date
              FROM invoices
              WHERE(NOT
                CASE WHEN invoices.actual_departure_date IS NULL THEN
                  EXISTS(SELECT exchange_rate
                  FROM exchange_rates e WHERE(e.from_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'USD')
                                          AND e.to_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'ZAR')
                                          AND e.roe_date = invoices.estimate_departure_date))
                ELSE
                  EXISTS(SELECT exchange_rate
                  FROM exchange_rates e WHERE(e.from_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'USD')
                                          AND e.to_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'ZAR')
                                          AND e.roe_date = invoices.actual_departure_date))
                END);
          STR
        else
          ''
        end
    return unless @send_mail

    Globals.send_an_email("#{@extract_name} extract for #{Date.today} - FAILED",
                          error_mail_recipients,
                          "An exception occurred.\n\n The error message is \"#{err.message}\"\n#{err.backtrace.join("\n")}\n\n#{s}")
  end
end

# Store constants from ActiveRecord CostGroup
class CostGroup
  RPT_GRP_OTHER   = 'other'
  RPT_GRP_CGA     = 'cga_levies'
  RPT_GRP_FOB     = 'fob_costs'
  RPT_GRP_FREIGHT = 'freight'
  RPT_GRP_LEVIES  = 'levies'
  RPT_GRP_OSEAS   = 'overseas_costs'
  RPT_GRP_PACK    = 'packing_costs'
  RPT_GRP_PPECB   = 'ppecb_levies'
  RPT_GRP_VAT     = 'vat'
  RPT_GRP_CS      = 'cold_storage'
  RPT_GRP_TPORT   = 'transport_to_port'
  RPT_PH_HAND     = 'ph_handling'
  RPT_PH_DGREEN   = 'ph_degreening'
  RPT_PH_PCOSTS   = 'ph_packing_costs'
  RPT_PH_CSTORE   = 'ph_cold_storage'
  RPT_GRP_REBATE  = 'rebate'
end

# Extract full season
class ExportFullSeasonWithCopy < ExportTask
  attr_accessor :voyage_year

  OUT_DIR = File.expand_path(ClientSettings.full_seasons_out_dir)
  ZERO    = BigDecimal('0')
  VAT_CHANGE_DATE = Date.new(2018, 4, 1)
  BLY_UPLOAD = '/media/win_ub_store/'

  # For development - return the full season SQL as a String.
  # Call from Pry or IRB.
  def self.show_query_sql(inv_type = :invoice)
    # usd_curr_id = ActiveRecord::Base.connection.select_value("SELECT c.id from currencies c WHERE c.currency_code = 'USD'")
    usd_curr_id = DB[:currencies].where(currency_code: 'USD').get(:id)
    # zar_curr_id = ActiveRecord::Base.connection.select_value("SELECT c.id from currencies c WHERE c.currency_code = 'ZAR'")
    zar_curr_id = DB[:currencies].where(currency_code: 'ZAR').get(:id)
    voyage_year = Date.today.year
    exporter = new("dw_fin_full_season_#{voyage_year}", 'Dummy test')
    exporter.voyage_year = voyage_year
    exporter.send(:query_for, inv_type, usd_curr_id, zar_curr_id)
  end

  # Extract the spreadsheet and upload to BUSII server.
  def self.upload_spreadsheet(voyage_year)
    RAILS_DEFAULT_LOGGER.info "Calling upload spreadsheet, #{voyage_year}"
    make_spreadsheet(voyage_year, true, true)
  end

  def self.make_spreadsheet(voyage_year, re_populate = true, upload_file = false)
    exporter             = new("dw_fin_full_season_#{voyage_year}", "Full Season DB #{voyage_year}")
    exporter.voyage_year = voyage_year
    RAILS_DEFAULT_LOGGER.info "Calling make spreadsheet, #{voyage_year}: re_populate: #{re_populate}, upload_file: #{upload_file}"
    exporter.make_spreadsheet(re_populate, upload_file)
  end

  def self.re_populate_table(voyage_year)
    exporter             = new("dw_fin_full_season_#{voyage_year}", "Full Season DB #{voyage_year}")
    exporter.voyage_year = voyage_year
    exporter.re_populate_table
  end

  def make_spreadsheet(re_populate = true, upload_file = false) # rubocop:disable Metrics/AbcSize, Metrics/PerceivedComplexity , Metrics/CyclomaticComplexity
    return if re_populate && !re_populate_table(false) # ---> THIS re-populates the table...

    out_file = if File.expand_path(__FILE__).include?('test')
                 "test_full_season_report_#{voyage_year}.csv"
               else
                 "full_season_report_#{voyage_year}.csv"
               end
    RAILS_DEFAULT_LOGGER.info "Make spreadsheet, re_populate: #{re_populate}, upload_file: #{upload_file}"

    force_text   = %i[pallet_number pallet_number_adjusted]
    column_names = DB.schema(@table_name.to_sym).map(&:first).reject { |c| %i[id created_at updated_at].include?(c) }.map(&:to_s)
    # column_names = ReportFullSeason.column_names.dup # GET FROM DB
    # column_names = column_names.delete_if { |c| %w[created_at updated_at id].include?(c) }

    # Sort columns in the same sequence as the YML report.
    # report_file  = "#{Globals.get_reports_location}/full_season.yml"
    report_file  = './full_season.yml'
    yml          = YAML.safe_load(File.read(report_file))
    stat         = yml['query'].gsub("\n", ' ')
    columns_list = list_of_cols_from_stat(stat)
    new_list     = []
    columns_list.each do |col|
      new_list << column_names.delete(col)
    end
    column_names = new_list.compact + column_names

    # headers      = column_names.map { |c| @inflector.titleize(c.gsub('_', ' ')).gsub(/Pol|Pod|Eta|Etd|Ata/, &:upcase) }
    headers      = column_names.map { |c| c.split('_').map(&:capitalize).join(' ').gsub(/Pol|Pod|Eta|Etd|Ata/, &:upcase) }

    log_step "Report make spreadsheet for #{@table_name} query started."

    files = { main: { file_name: File.join(OUT_DIR, out_file) } }
    log_step "Main output file is: #{files[:main][:file_name]} "

    super_group_file_keys = {}
    # Track which Super Groups to extract for.
    # Note: 2 super groups can be written to the same super group file.
    #       e.g. DUN-SW,UDC-SW  :: Both super groups will be written to the DUN-SW file.
    ClientSettings.full_season_super_group_splits.each do |super_group|
      ar = super_group.split(',')
      sg_key = ar.first
      ar.each { |a| super_group_file_keys[a] = sg_key }
      sg_file                   = out_file.sub('.csv', "_#{sg_key}.csv")
      files[sg_key]             = {}
      files[sg_key][:file_name] = File.join(OUT_DIR, sg_file)
    end

    ClientSettings.full_season_marketing_desk_splits.each do |marketing_desk|
      sg_file                           = out_file.sub('.csv', "_#{marketing_desk.gsub(' ', '_')}.csv")
      files[marketing_desk]             = {}
      files[marketing_desk][:file_name] = File.join(OUT_DIR, sg_file)
    end

    # Create the CSV files
    files.each_key do |key|
      files[key][:csv_file] = CSV.open(files[key][:file_name], 'w')
      files[key][:csv_file] << headers
    end

    # rows = ActiveRecord::Base.connection.select_all("SELECT * FROM #{@table_name}")
    rows = DB[@table_name.to_sym].all

    # Iterate through all rows exporting to relevant files.
    rows.each do |row|
      row.each_key do |k|
        row[k] = row[k].to_f if row[k].is_a?(BigDecimal)
        if BOOL_FIELDS.include?(k)
          # row[k] = 'true' if row[k] == 't'
          # row[k] = 'false' if row[k] == 'f'
          row[k] = 't' if row[k] == true
          row[k] = 'f' if row[k] == false
        end
      end
      files[:main][:csv_file] << column_names.map { |f| force_text.include?(f.to_sym) ? "'#{row[f.to_sym]}" : row[f.to_sym] }
      if super_group_file_keys[row[:supplier_super_group]]
        key = super_group_file_keys[row[:supplier_super_group]]
        files[key][:csv_file] << column_names.map { |f| force_text.include?(f.to_sym) ? "'#{row[f.to_sym]}" : row[f.to_sym] }
      end
      if files[row[:marketing_desk]]
        files[row[:marketing_desk]][:csv_file] << column_names.map { |f| force_text.include?(f.to_sym) ? "'#{row[f.to_sym]}" : row[f.to_sym] }
      end
    end

    # Close CSV files
    files.each_key { |key| files[key][:csv_file].close }

    if ClientSettings.rsync_target && !DEV_MODE
      base = ClientSettings.full_seasons_out_dir.sub('~', '')
      files.each_key do |key|
        log_step "Rsync file #{key}."
        `rsync -az #{files[key][:file_name]} 172.16.0.17:/home/unifrutti#{base}/`
      end
    end

    if upload_file
      log_step 'Full Season extract complete.'
      # copy_to_busii_parallel(File.join(OUT_DIR, out_file)) if ClientSettings.bi_upload_parallel
      # copy_to_busii(File.join(OUT_DIR, out_file))
      copy_to_bly(files['BLY-SW'][:file_name]) if ClientSettings.bi_upload_bly && !DEV_MODE
      copy_to_bly(files['UBC-SW'][:file_name]) if ClientSettings.bi_upload_bly && !DEV_MODE
      log_step 'Full Season uploads complete.', true
    else
      log_step 'Full Season extract complete.', true
    end
  rescue StandardError => e
    log_step "Report make spreadsheet #{@table_name} - EXCEPTION: #{e.message}."
    handle_error(e)
  end

  # def copy_to_busii(file_name)
  #   log_step 'Copying full season spreadsheet to BUSII server.'
  #
  #   `scp -i ~/.ssh/id_rsa #{file_name} #{ClientSettings.bi_upload_server}:#{ClientSettings.bi_upload_path}`
  #
  #   if $? == 0
  #     log_step 'Copied full season spreadsheet to BUSII server.', true
  #     `ssh -i ~/.ssh/id_rsa #{ClientSettings.bi_upload_server} touch #{File.join(ClientSettings.bi_upload_path, 'fsr.ready')}`
  #   else
  #     log_step "Failed to copy spreadsheet to BUSII server: #{$?.exitstatus}."
  #   end
  # end
  #
  # def copy_to_busii_parallel(file_name)
  #   log_step 'Copying full season spreadsheet to BUSII parallel server.'
  #
  #   `scp -i ~/.ssh/id_rsa #{file_name} #{ClientSettings.bi_upload_parallel}:#{ClientSettings.bi_upload_path}`
  #
  #   if $? == 0
  #     log_step 'Copied full season spreadsheet to BUSII parallel server.'
  #     `ssh -i ~/.ssh/id_rsa #{ClientSettings.bi_upload_parallel} touch #{File.join(ClientSettings.bi_upload_path, 'fsr.ready')}`
  #   else
  #     log_step "Failed to copy spreadsheet to BUSII parallel server: #{$?.exitstatus}."
  #   end
  # end

  def copy_to_bly(file_name)
    log_step "Copying full season spreadsheet to BLY share. [#{File.basename(file_name)}]"
    `cp #{file_name} #{BLY_UPLOAD}`
  end

  def re_populate_table(log_completion = true)
    @csv_file_path = File.join(ClientSettings.postgres_workarea, "pg_copy_fs_#{voyage_year}.csv")
    # @csv_file_path = '/vagrant/pgtmp/pg_copy_fs_2020.csv'
    @row_count = 0
    # @send_mail = false # JS testing...

    log_step "Report re-populate for #{@table_name} invoices query started."

    cre = "CREATE TEMPORARY TABLE dw_temp_fs (LIKE #{@table_name}); ALTER TABLE dw_temp_fs DROP COLUMN id;"
    # ActiveRecord::Base.connection.execute(cre)
    DB.run(cre)

    # Delete the csv file so that it can be appended-to
    File.delete(@csv_file_path) if File.exist?(@csv_file_path)

    # usd_curr_id = ActiveRecord::Base.connection.select_value("SELECT c.id from currencies c WHERE c.currency_code = 'USD'")
    usd_curr_id = DB[:currencies].where(currency_code: 'USD').get(:id)
    # zar_curr_id = ActiveRecord::Base.connection.select_value("SELECT c.id from currencies c WHERE c.currency_code = 'ZAR'")
    zar_curr_id = DB[:currencies].where(currency_code: 'ZAR').get(:id)

    @first_call = true
    populate_from_invoices(usd_curr_id, zar_curr_id)
    @first_call = false

    log_step "Report re-populate for #{@table_name} preliminary invoices query started."
    populate_from_preliminaries(usd_curr_id, zar_curr_id)

    log_step "Report re-populate for #{@table_name} copy from temporary table started."

    # UNDO COPY: upd = "DELETE FROM #{@table_name}"
    upd = "TRUNCATE TABLE #{@table_name}"
    # ActiveRecord::Base.connection.execute(upd)
    DB.run(upd)

    # disable indexes?
    upd = "INSERT INTO #{@table_name} (#{COL_NAMES.join(',')}) SELECT #{COL_NAMES.join(',')} FROM dw_temp_fs;"
    # ActiveRecord::Base.connection.execute(upd)
    DB.run(upd)

    log_step "Report re-populate for #{@table_name} ended.", log_completion
    true
  rescue StandardError => e
    log_step "Report re-populate for #{@table_name} - EXCEPTION: #{e.message}."
    handle_error(e)
    false
  end

  private

  # Run a query to get full season report data for PalletSequences that have been invoiced.
  def populate_from_invoices(usd_curr_id, zar_curr_id)
    qry = query_for(:invoice, usd_curr_id, zar_curr_id)
    # recs = ActiveRecord::Base.connection.select_all(qry)
    recs = DB[qry].all

    log_step "Report re-populate for #{@table_name} query ended and write to csv started."

    bulk_insert(recs)

    log_step "Report re-populate for #{@table_name} invoices csv created and COPIED #{@record_count} records to temp table."
  end

  # Run a query to get full season report data for PalletSequences that have NOT been invoiced, but have preliminary invoices.
  def populate_from_preliminaries(usd_curr_id, zar_curr_id)
    # qry = ExportFullSeasonReport.query_for(:preliminary_invoice, usd_curr_id, zar_curr_id)
    qry = query_for(:preliminary_invoice, usd_curr_id, zar_curr_id)
    # recs = ActiveRecord::Base.connection.select_all(qry)
    recs = DB[qry].all

    log_step "Report re-populate for #{@table_name} query ended and write to csv started."

    bulk_insert(recs)

    log_step "Report re-populate for #{@table_name} preliminary invoices csv created and COPIED #{@record_count} records to temp table."
  end

  # Go through the result set and populate rows in SqlScript for bulk insert.
  def bulk_insert(recs) # rubocop:disable Metrics/AbcSize, Metrics/PerceivedComplexity, Metrics/CyclomaticComplexity
    @record_count = 0
    # FasterCSV.open(@csv_file_path, 'w') do |csv|
    #   csv << COL_NAMES
    # end
    CSV.open(@csv_file_path, 'a') do |csv|
      csv << COL_NAMES if @first_call
      recs.each do |rec|
        @record_count += 1
        row = {}

        INT_FIELDS.each       { |f| row[f] = rec[f].nil? ? nil   : rec[f].to_i        }
        DATE_FIELDS.each      { |f| row[f] = rec[f].nil? ? nil   : rec[f].to_date     }
        # DATE_TIME_FIELDS.each { |f| row[f] = rec[f].nil? ? nil   : Time.parse(rec[f]) }
        DATE_TIME_FIELDS.each { |f| row[f] = rec[f].nil? ? nil   : rec[f]             }
        BOOL_FIELDS.each      { |f| row[f] = rec[f].nil? ? false : rec[f]             }
        NUM_FIELDS.each       { |f| row[f] = rec[f].nil? ? nil   : rec[f]             }
        STR_FIELDS.each       { |f| row[f] = rec[f].nil? ? nil   : rec[f].strip       }

        make_calculations_for_bulk(row, rec)

        # NUM_COLS.each do |c|
        #   tmp = row[c].to_s
        #   if tmp.length > 20 # Reduce number of digits after decimal
        #     row[c] = BigDecimal(tmp).round(20)
        #   end
        # end

        # Insert row into dw_temp_fs
        # FasterCSV.open(@csv_file_path, 'a') do |csv|
        #   csv << COL_NAMES.map { |c| convert_value(c, row[c]) }
        # end
        csv << COL_NAMES.map { |c| convert_value(c, row[c]) }
      end
    end

    upd = "COPY dw_temp_fs (#{COL_NAMES.join(',')}) FROM '#{@csv_file_path}' WITH (FORMAT csv, HEADER TRUE);"
    # upd = "COPY dw_temp_fs (#{COL_NAMES.join(',')}) FROM '/home/james/ra/unifrutti_exporter/pgtmp/pg_copy_fs_2020.csv' WITH (FORMAT csv, HEADER TRUE);"
    # ActiveRecord::Base.connection.execute(upd) if @record_count.positive?
    DB.run(upd) if @record_count.positive?
  end

  # # Convert SqlScript to SQL text wrapped in a transaction and execute it.
  # def write_to_db(sql_script)
  #   ActiveRecord::Base.connection.execute(sql_script.to_script)
  # end

  def convert_value(column, value)
    if FIELD_DEFS[column] == :numeric
      tmp = value.to_s
      if tmp.length > 20 # Reduce number of digits after decimal
        BigDecimal(tmp).round(20)
      else
        value
      end
    else
      value
    end
  end

  # Perform various calculations on fields.
  def make_calculations_for_bulk(row, rec)
    pay_in_forex = rec[:pay_in_forex] == true # 't'
    # START FIX ---
    # In Nov 2014, 18 credit notes were created that had positive unit prices.
    # This fix is in place to compensate for the change in the query where the ABS functions were removed.
    # The fix can be reversed when the full season does not include invoices from Nov 2014.
    # NOTE: quality credit not is an absolute value.
    credit_note                             = rec[:credit_note]       * -1 unless rec[:credit_note].nil? # ...quality?...
    total_credit_note                       = rec[:total_credit_note] * -1 unless rec[:total_credit_note].nil?
    # -- END FIX
    row[:credit_note]                       = credit_note
    row[:total_credit_note]                 = total_credit_note

    row[:pallet_number_adjusted]            = rec[:pallet_number].gsub('_', '') # Remove underscores from pallet numbers
    cif_fob                                 = (rec[:invoiced_price_per_carton] || ZERO)  -
                                              ((rec[:quality_credit_note] || ZERO)       + (credit_note || ZERO))               +
                                              (rec[:debit_note] || ZERO)
    row[:cif_fob]                           = cif_fob
    total_cif_fob                           = (rec[:total_invoiced_price] || ZERO)       -
                                              ((rec[:total_quality_credit_note] || ZERO) + (total_credit_note || ZERO))         +
                                              (rec[:total_debit_note] || ZERO)
    row[:total_cif_fob]                     = total_cif_fob
    pre_profit_fob                          = cif_fob - (rec[:freight] || ZERO)          - (rec[:overseas_costs] || ZERO)      -
                                              (rec[:provision_commercial_credit] || ZERO)
    total_cartons                           = rec[:total_cartons].to_i
    row[:pre_profit_fob]                    = pre_profit_fob
    row[:total_pre_profit_fob]              = pre_profit_fob * total_cartons
    perc                                    = rec[:sas_perc] || rec[:perc] # Accountsale percentage trumps customer percentage.
    row[:perc]                              = perc
    latest_roe                              = rec[:roe_on_acc_sale] || rec[:roe_on_atd] || rec[:roe_on_etd] # Use Accsale ROE if present
    row[:quality_credit_note_zar]           = rec[:quality_credit_note] * latest_roe unless rec[:quality_credit_note].nil?
    row[:total_quality_credit_note_zar]     = rec[:total_quality_credit_note] * latest_roe unless rec[:total_quality_credit_note].nil?
    ci_vat_factor                           = rec[:customer_invoice_date] < VAT_CHANGE_DATE ? 0.14 : 0.15

    forex_local_rate = if pay_in_forex
                         (rec[:roe_on_atd] || rec[:roe_on_etd])
                       else
                         latest_roe
                       end

    profit = if rec[:deal_category] == 'FIXED' && !perc.nil?
               (pre_profit_fob || ZERO) * perc
             else
               ZERO
             end
    row[:profit]                            = profit

    row[:total_profit]                      = profit * total_cartons
    post_profit_fob                         = pre_profit_fob - profit
    row[:post_profit_fob]                   = post_profit_fob
    row[:total_post_profit_fob]             = post_profit_fob * total_cartons

    commission = if rec[:deal_category] == 'COMMISSION' && !perc.nil?
                   post_profit_fob * perc
                 else
                   ZERO
                 end
    row[:commission] = commission

    row[:total_commission]                  = commission * total_cartons
    supplier_fob                            = post_profit_fob - commission
    row[:supplier_fob]                      = supplier_fob
    row[:total_supplier_fob]                = supplier_fob * total_cartons
    actual_fob_costs_per_ctn                = (rec[:actual_fob_costs_per_carton_zar] || ZERO) / forex_local_rate
    row[:actual_fob_costs_per_ctn]          = actual_fob_costs_per_ctn
    row[:total_actual_fob_costs]            = actual_fob_costs_per_ctn * total_cartons
    row[:total_actual_fob_costs_zar]        = (rec[:actual_fob_costs_per_carton_zar] || ZERO) * total_cartons
    est_cost                                = (rec[:accumulated_est_cost] || BigDecimal('0.0'))
    row[:total_estimated_fob_costs_zar]     = est_cost * BigDecimal((rec[:pallet_size] || '0.0').to_s)

    estimated_fob_cost_per_carton_zar       = (est_cost * BigDecimal((rec[:pallet_size] || '0.0').to_s)) / total_cartons
    row[:estimated_fob_cost_per_carton_zar] = estimated_fob_cost_per_carton_zar
    estimated_fob_costs_per_crtn            = estimated_fob_cost_per_carton_zar / forex_local_rate
    row[:estimated_fob_costs_per_crtn]      = estimated_fob_costs_per_crtn
    row[:total_estimated_fob_costs]         = estimated_fob_costs_per_crtn * total_cartons

    cs_costs_zar                            = rec[:cold_storage_cost_per_carton_zar] || ZERO

    rebate_zar                              = rec[:rebate_zar] || ZERO
    rebate                                  = rebate_zar / forex_local_rate
    row[:rebate_zar]                        = rebate_zar
    row[:total_rebate_zar]                  = rebate_zar * total_cartons
    row[:rebate]                            = rebate
    row[:total_rebate]                      = rebate * total_cartons

    ex_cold_store_per_carton                = supplier_fob - estimated_fob_costs_per_crtn - actual_fob_costs_per_ctn - rebate
    row[:ex_cold_store_per_carton]          = ex_cold_store_per_carton
    row[:total_ex_cold_store]               = ex_cold_store_per_carton * total_cartons

    dip                                     = ex_cold_store_per_carton - (cs_costs_zar / forex_local_rate)
    row[:dip]                               = dip
    row[:total_dip]                         = dip                  * total_cartons
    pre_profit_fob_zar                      = pre_profit_fob       * forex_local_rate
    row[:pre_profit_fob_zar]                = pre_profit_fob_zar
    total_pre_profit_fob_zar                = (pre_profit_fob      * forex_local_rate) * total_cartons
    row[:total_pre_profit_fob_zar]          = total_pre_profit_fob_zar
    profit_zar                              = profit               * forex_local_rate
    row[:profit_zar]                        = profit_zar
    row[:total_profit_zar]                  = profit_zar           * total_cartons
    post_profit_fob_zar                     = (pre_profit_fob      * forex_local_rate) - profit_zar
    row[:post_profit_fob_zar]               = post_profit_fob_zar
    row[:total_post_profit_fob_zar]         = post_profit_fob_zar  * total_cartons
    commission_zar                          = commission           * forex_local_rate
    row[:commission_zar]                    = commission_zar
    row[:total_commission_zar]              = (commission          * forex_local_rate) * total_cartons
    row[:supplier_fob_zar]                  = post_profit_fob_zar  - commission_zar
    row[:total_supplier_fob_zar]            = (post_profit_fob_zar - commission_zar)   * total_cartons

    ex_cold_store_per_carton_zar            = (post_profit_fob_zar - commission_zar) -
                                              estimated_fob_cost_per_carton_zar      -
                                              (rec[:actual_fob_costs_per_carton_zar] || ZERO) -
                                              rebate_zar
    row[:ex_cold_store_per_carton_zar]      = ex_cold_store_per_carton_zar
    row[:total_ex_cold_store_zar]           = ex_cold_store_per_carton_zar * total_cartons

    dip_zar                                 = ex_cold_store_per_carton_zar - cs_costs_zar
    row[:dip_zar]                           = dip_zar
    row[:total_dip_zar]                     = dip_zar                        * total_cartons
    row[:total_freight]                     = (rec[:freight]       || ZERO) * total_cartons

    ppecb_zar                               = rec[:ppecb_zar]         || ZERO
    cga_zar                                 = rec[:cga_zar]           || ZERO
    other_levies_zar                        = rec[:other_levies_zar]  || ZERO
    packing_costs_zar                       = rec[:packing_costs_zar] || ZERO
    trans_port_costs_zar                    = rec[:transport_to_port_per_carton_zar] || ZERO

    row[:total_ppecb_zar]                   = ppecb_zar             * total_cartons
    row[:total_cga_zar]                     = cga_zar               * total_cartons
    row[:total_other_zar]                   = other_levies_zar      * total_cartons
    row[:total_packing_costs_zar]           = packing_costs_zar     * total_cartons

    row[:ppecb]                             = ppecb_zar.zero?         ? ZERO : ppecb_zar         / forex_local_rate
    row[:cga]                               = cga_zar.zero?           ? ZERO : cga_zar           / forex_local_rate
    row[:other_levies]                      = other_levies_zar.zero?  ? ZERO : other_levies_zar  / forex_local_rate
    row[:packing_costs]                     = packing_costs_zar.zero? ? ZERO : packing_costs_zar / forex_local_rate
    row[:total_ppecb]                       = ppecb_zar.zero?         ? ZERO : ppecb_zar         * total_cartons / forex_local_rate
    row[:total_cga]                         = cga_zar.zero?           ? ZERO : cga_zar           * total_cartons / forex_local_rate
    row[:total_other]                       = other_levies_zar.zero?  ? ZERO : other_levies_zar  * total_cartons / forex_local_rate
    row[:total_packing_costs]               = packing_costs_zar.zero? ? ZERO : packing_costs_zar * total_cartons / forex_local_rate

    row[:total_cold_storage_costs_zar]      = cs_costs_zar * total_cartons
    row[:cold_storage_cost_per_carton]      = cs_costs_zar / forex_local_rate
    row[:total_cold_storage_costs]          = cs_costs_zar / forex_local_rate * total_cartons

    row[:total_transport_to_port_zar]       = trans_port_costs_zar * total_cartons
    row[:transport_to_port_per_carton]      = trans_port_costs_zar / forex_local_rate
    row[:total_transport_to_port]           = trans_port_costs_zar / forex_local_rate * total_cartons

    vat_zar = if rec[:to_be_charged_vat] == true # 't'
                commission_zar * ci_vat_factor
              else
                BigDecimal('0')
              end
    vatable_costs                           = (rec[:vatable_costs] || ZERO)
    vat_zar                                += vatable_costs * ci_vat_factor # This SHOULD be based on the SPI date, but "which one?"
    row[:vat_zar]                           = vat_zar
    row[:total_vat_zar]                     = vat_zar * total_cartons
    due_to_supplier_per_carton_zar          = dip_zar - trans_port_costs_zar -
                                                        ppecb_zar            -
                                                        cga_zar              -
                                                        other_levies_zar     -
                                                        packing_costs_zar    -
                                                        vat_zar
    row[:due_to_supplier_per_carton_zar]    = due_to_supplier_per_carton_zar
    row[:total_due_to_supplier_zar]         = due_to_supplier_per_carton_zar * total_cartons

    row[:vat]                               = vat_zar                        / forex_local_rate
    row[:total_vat]                         = (vat_zar * total_cartons)      / forex_local_rate
    row[:due_to_supplier_per_carton]        = due_to_supplier_per_carton_zar / forex_local_rate
    row[:total_due_to_supplier]             = (due_to_supplier_per_carton_zar * total_cartons) / forex_local_rate

    ph_handling                             = rec[:ph_handling]      || ZERO
    ph_degreening                           = rec[:ph_degreening]    || ZERO
    ph_packing_costs                        = rec[:ph_packing_costs] || ZERO
    ph_cold_storage                         = rec[:ph_cold_storage]  || ZERO

    row[:ph_handling]                       = ph_handling
    row[:total_ph_handling]                 = ph_handling * total_cartons
    row[:ph_degreening]                     = ph_degreening
    row[:total_ph_degreening]               = ph_degreening * total_cartons
    row[:ph_packing_costs]                  = ph_packing_costs
    row[:total_ph_packing_costs]            = ph_packing_costs * total_cartons
    row[:ph_cold_storage]                   = ph_cold_storage
    row[:total_ph_cold_storage]             = ph_cold_storage * total_cartons

    ph_vatable_costs                        = (rec[:ph_vatable_costs] || ZERO)
    ph_vat                                  = ph_vatable_costs * ci_vat_factor
    row[:ph_vat]                            = ph_vat
    row[:total_ph_vat]                      = ph_vat * total_cartons

    ph_supplier_nett                        = due_to_supplier_per_carton_zar - ph_handling      -
                                                                               ph_degreening    -
                                                                               ph_packing_costs -
                                                                               ph_cold_storage  - ph_vat
    row[:ph_supplier_nett]                  = ph_supplier_nett
    row[:total_ph_supplier_nett]            = ph_supplier_nett * total_cartons

    row[:zar_total_sales]                   = total_cif_fob * rec[:roe_on_atd]
    row[:total_usd_invoiced]                = row[:zar_total_sales] / rec[:usd_etd_roe]

    if rec[:currency_code] == 'USD'
      row[:usd_sales]                         = pre_profit_fob
      row[:total_usd_sales]                   = pre_profit_fob * total_cartons
    else
      row[:usd_sales]                         = pre_profit_fob_zar / rec[:usd_etd_roe]
      row[:total_usd_sales]                   = total_pre_profit_fob_zar / rec[:usd_etd_roe]
    end

    # SIDENOTE:
    # If an invoice has an ETD > 5 days ahead, a division by NULL usd_etd_roe will raise an exception.
    #
    # To find the invoice(s) in error, run the following SQL:
    #
    #   SELECT invoices.id, invoices.invoice_ref_no,
    #   invoices.estimate_departure_date, invoices.actual_departure_date
    #   FROM invoices
    #   WHERE(NOT
    #     CASE WHEN invoices.actual_departure_date IS NULL THEN
    #       EXISTS(SELECT exchange_rate
    #       FROM exchange_rates e WHERE(e.from_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'USD')
    #                               AND e.to_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'ZAR')
    #                               AND e.roe_date = invoices.estimate_departure_date))
    #     ELSE
    #       EXISTS(SELECT exchange_rate
    #       FROM exchange_rates e WHERE(e.from_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'USD')
    #                               AND e.to_currency_id = (SELECT c.id from currencies c WHERE c.currency_code = 'ZAR')
    #                               AND e.roe_date = invoices.actual_departure_date))
    #     END);
  rescue StandardError => e
    RAILS_DEFAULT_LOGGER.info e.message
    RAILS_DEFAULT_LOGGER.info e.backtrace.join("\n")
    raise
  end

  # Build up the SQL - either for invoices or preliminaries.
  # There is enough commonality in both queries to need one set of code to build it up
  # (otherwise it is too easy to modify one version and not the other)
  # However there are some big differences as well - hence the variables set at
  # the top of the method depending on invoice type...
  def query_for(inv_type, usd_curr_id, zar_curr_id)
    if inv_type == :invoice
      invoice_prefix = 'invoices'
      # invoice_id     = 'invoice_id'
      item_prefix    = 'item'
      extra_where    = ''
      inv_ref        = 'invoices.invoice_ref_no,'
      invoice_joins  = <<-SQL
        JOIN invoices ON invoices.id = pallet_sequences.invoice_id
        JOIN invoice_items item ON item.id = pallet_sequences.invoice_item_id
        LEFT OUTER JOIN invoices preliminary_inv ON preliminary_inv.id = pallet_sequences.preliminary_invoice_id
        LEFT OUTER JOIN invoice_items preliminary_inv_item ON preliminary_inv_item.id = pallet_sequences.preliminary_invoice_item_id
      SQL
      invoice_prices = <<-SQL
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.unit_price AS numeric(13,6))
        ELSE
          NULL
        END AS invoiced_price_per_kg,
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                          fg_product_weights_suppliers.nett_weight,
                                          fg_product_weights.nett_weight) AS numeric(13,6))
        ELSE
          CAST(item.unit_price AS numeric(12,6))
        END AS invoiced_price_per_carton,
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                          fg_product_weights_suppliers.nett_weight,
                                          fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS numeric(13,6))
        ELSE
          CAST(item.unit_price * pallet_sequences.carton_quantity AS numeric(13,6))
        END AS total_invoiced_price,
        -- Expected invoice prices
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.expected_invoice_price AS numeric(13,6))
        ELSE
          NULL
        END AS expected_invoiced_price_per_kg,
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.expected_invoice_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                          fg_product_weights_suppliers.nett_weight,
                                          fg_product_weights.nett_weight) AS numeric(13,6))
        ELSE
          CAST(item.expected_invoice_price AS numeric(12,6))
        END AS expected_invoiced_price_per_carton,
        CASE WHEN invoices.price_is_per_kg THEN
          CAST(item.expected_invoice_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                          fg_product_weights_suppliers.nett_weight,
                                          fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS numeric(13,6))
        ELSE
          CAST(item.expected_invoice_price * pallet_sequences.carton_quantity AS numeric(13,6))
        END AS total_expected_invoiced_price,
      SQL
    else
      invoice_prefix = 'preliminary_inv'
      # invoice_id     = 'preliminary_invoice_id'
      item_prefix    = 'preliminary_inv_item'
      extra_where    = "pallet_sequences.invoice_id IS NULL AND pallet_sequences.preliminary_invoice_id IS NOT NULL\n AND "
      inv_ref        = 'NULL AS invoice_ref_no,'
      invoice_joins  = <<-SQL
        JOIN invoices preliminary_inv ON preliminary_inv.id = pallet_sequences.preliminary_invoice_id
        JOIN invoice_items preliminary_inv_item ON preliminary_inv_item.id = pallet_sequences.preliminary_invoice_item_id
      SQL
      invoice_prices = <<-SQL
        0.0 AS invoiced_price_per_kg,
        0.0 AS invoiced_price_per_carton,
        0.0 AS total_invoiced_price,
        -- Expected invoice prices
        0.0 AS expected_invoiced_price_per_kg,
        0.0 AS expected_invoiced_price_per_carton,
        0.0 AS total_expected_invoiced_price,
      SQL
    end

    <<~SQL
      WITH psize as
        ( SELECT pallet_sequences.id,
          SUM(CAST(pallet_sequences.carton_quantity as float) / (SELECT SUM(pt.carton_quantity)
              FROM pallet_sequences pt
              WHERE pt.pallet_id = pallet_sequences.pallet_id)) AS pallet_size
          FROM pallet_sequences
          INNER JOIN pallets ON pallet_sequences.pallet_id = pallets.id
          WHERE pallets.allocated IS true
            AND (pallets.exit_ref IS NULL OR upper(pallets.exit_ref::text) <> 'SCRAPPED'::text) AND pallet_sequences.pallet_number::text IS NOT NULL
          GROUP BY pallet_sequences.id
          ORDER BY pallet_sequences.pallet_id, pallet_sequences.id )

      SELECT
      pallet_sequences.id AS pallet_sequence_id, pallet_sequences.pallet_sequence_number,
      #{item_prefix}.low_chem,
      #{invoice_prefix}.id AS invoice_id,
      #{inv_ref}
      #{invoice_prefix}.ucr_number,
      #{invoice_prefix}.paid AS invoice_paid_in_full,
          #{invoice_prefix}.account_sale_due_date,
      cust.party_name AS customer_code,
      customer_gl_codes.gl_code,
      #{item_prefix}.final_receiver,
      to_char(COALESCE(#{invoice_prefix}.actual_departure_date, #{invoice_prefix}.estimate_departure_date), 'MONTH') AS month,
      to_char(COALESCE(#{invoice_prefix}.actual_departure_date, #{invoice_prefix}.estimate_departure_date), 'IW') AS shipping_week,
      CASE deal_types.deal_type WHEN 'FP' THEN 'FIXED' ELSE 'COMMISSION' END AS deal_category,
      deal_types.deal_type,
      inco_terms.inco_term AS receiver_inco_term,
      supp_inco_terms.inco_term AS supplier_inco_term,
      voyages.voyage_code,
      regexp_replace(replace(regexp_replace(voyages.voyage_code, '^20', ''), 'FAST_LOCAL_20', ''), '(_\\d+)_.+', '\\1') as voyage_short_code,
      #{item_prefix}.container_number,
      parties_sl.party_name AS shipping_line,
      #{invoice_prefix}.port_of_loading AS POL,
      #{invoice_prefix}.port_of_discharge AS POD,
      #{invoice_prefix}.estimate_arrival_date AS ETA,
      #{invoice_prefix}.estimate_departure_date AS ETD,
      #{invoice_prefix}.actual_departure_date AS ATD,
      j.arrival_date AS ATA,
      date_part('year', COALESCE(#{invoice_prefix}.actual_departure_date, #{invoice_prefix}.estimate_departure_date)) AS year,
      supplier_super_groups.supplier_super_group_code AS supplier_super_group,
      supplier_groups.supplier_group_code AS supplier_group,
      supp.party_name AS supplier,
      suppliers.to_be_charged_vat,
      COALESCE(pallet_sequences.qc_status, CASE WHEN #{invoice_prefix}.approved THEN NULL ELSE 'Invoice Not Approved' END) AS qc_status,
      qc_pallet_sequences.financial_qc_status,
      pallet_sequences.packed_date_time,
      to_char(pallet_sequences.packed_date_time, 'IW') AS packing_week,
      to_char(pallets.intake_date, 'IW') AS intake_week,
      pools.pool_name as pool,
      pallet_sequences.farm_code,
      pallet_sequences.puc,
      pucs.farm_within_farm,
      pallets.pallet_number,
      pallet_sequences.season_code,
      pallet_sequences.orchard,
      pallets.packhouse_code AS phc,
      load_instructions.depot_code AS seal_point,
      commodities.commodity_code,
      commodity_groups.commodity_group_description,
      varieties.variety_code,
      varieties.variety_description,
      variety_groups.variety_group,
      item_pack_products.size_count_code,
      COALESCE(marks.mark_name, marks.mark_code) AS mark_code,
      pallet_sequences.inventory_code,
      pallet_sequences.packed_tm_group,
      item_pack_products.grade_code,
      COALESCE(pallet_sequences.fg_product_nett_weight,
              fg_product_weights_suppliers.nett_weight,
              fg_product_weights.nett_weight) AS nett_weight,
      COALESCE(pallet_sequences.fg_product_nett_weight,
              fg_product_weights_suppliers.nett_weight,
              fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS total_weight,
      COALESCE(fg_product_weights_suppliers.nett_weight,
              fg_product_weights.nett_weight) AS nett_masterfile_weight,
      COALESCE(fg_product_weights_suppliers.nett_weight,
              fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS total_masterfile_weight,
      CASE WHEN pallet_sequences.mark_code = 'KGS' THEN
        COALESCE(pallet_sequences.fg_product_nett_weight,
                fg_product_weights_suppliers.nett_weight,
                fg_product_weights.nett_weight)::numeric
      ELSE
        fg_product_weights.standard_carton_nett_weight
      END AS commercial_weight,
      CASE WHEN pallet_sequences.mark_code = 'KGS' THEN
        COALESCE(pallet_sequences.fg_product_nett_weight,
                fg_product_weights_suppliers.nett_weight,
                fg_product_weights.nett_weight)::numeric
       * pallet_sequences.carton_quantity
      ELSE
        fg_product_weights.standard_carton_nett_weight * pallet_sequences.carton_quantity
      END AS total_commercial_weight,
      fg_products.pack_code,
      pallet_sequences.shipping_tm_group,
      target_market_groups.description AS sale_tm,
      marketing_tm.description AS marketing_tm_description,
      pallet_sequences.fin_tm_group AS fin_tm,
      marketing_desks.marketing_desk,
      CASE deal_types.deal_type WHEN 'FP' THEN 'PROFIT' ELSE 'COMMISSION' END AS profit_commission,
      customers.unifrutti_commission_percent / 100.0 AS perc,
      supplier_account_sales.unifrutti_commission_percent / 100.0 AS sas_perc,
      supplier_account_sales.pay_in_forex,
      psize.pallet_size,
      pallet_sequences.carton_quantity AS total_cartons,
      CASE WHEN pallet_sequences.mark_code = 'KGS' AND commodity_groups.commodity_group_code = 'CITRUS' THEN
        (COALESCE(pallet_sequences.fg_product_nett_weight,
                fg_product_weights_suppliers.nett_weight,
          fg_product_weights.nett_weight)::numeric(20,4)
        * pallet_sequences.carton_quantity
        / CASE WHEN commodities.commodity_code = 'SC' THEN 10 ELSE 15 END)::integer
      WHEN pallet_sequences.mark_code = 'KGS' AND commodities.commodity_code = 'MA' THEN
        (COALESCE(pallet_sequences.fg_product_nett_weight,
                fg_product_weights_suppliers.nett_weight,
                fg_product_weights.nett_weight)
        * pallet_sequences.carton_quantity
  / 4.2)::numeric(20,4)
      ELSE
  (pallet_sequences.carton_quantity / fg_product_weights.ratio_to_standard_carton)::numeric(20,4)
      END AS standard_cartons,
      currencies.currency_code,
      supp_currencies.currency_code AS supplier_currency,
      CASE WHEN #{invoice_prefix}.actual_departure_date IS NULL THEN
        (SELECT exchange_rate FROM exchange_rates e WHERE e.from_currency_id = #{usd_curr_id}
                                                    AND e.to_currency_id = #{zar_curr_id}
                                                    AND e.roe_date = #{invoice_prefix}.estimate_departure_date)
      ELSE
        (SELECT exchange_rate FROM exchange_rates e WHERE e.from_currency_id = #{usd_curr_id}
                                                    AND e.to_currency_id = #{zar_curr_id}
                                                    AND e.roe_date = #{invoice_prefix}.actual_departure_date)
      END AS usd_etd_roe,
      exchange_rate_est.exchange_rate AS roe_on_etd,
      exchange_rate_actual.exchange_rate AS roe_on_atd,
      supplier_account_sales.exchange_rate AS roe_on_acc_sale,
(SELECT (SUM(cri.amount * cr.rate_of_exchange) / NULLIF(SUM(cri.amount), 0))::numeric(11,4)
       FROM customer_receipt_items cri
       JOIN customer_receipts cr ON cr.id = cri.customer_receipt_id
       WHERE cri.invoice_id IN (SELECT id FROM invoices recinv WHERE recinv.id = #{invoice_prefix}.id OR recinv.original_invoice_id = #{invoice_prefix}.id)
         AND cr.completed) AS roe_on_receipt,
      supplier_account_sales.id AS supp_accsale_id,
      #{invoice_prices}

      preliminary_inv.invoice_ref_no AS preliminary_ref_no,

      CASE WHEN preliminary_inv.price_is_per_kg THEN
  CAST(preliminary_inv_item.unit_price AS numeric(20,6))
      ELSE
        NULL
      END AS preliminary_price_per_kg,
      CASE WHEN preliminary_inv.price_is_per_kg THEN
        CAST(preliminary_inv_item.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                fg_product_weights_suppliers.nett_weight,
                                                fg_product_weights.nett_weight) AS numeric(20,6))
      ELSE
        CAST(preliminary_inv_item.unit_price AS numeric(20,6))
      END AS preliminary_price_per_carton,
      CASE WHEN preliminary_inv.price_is_per_kg THEN
        CAST(preliminary_inv_item.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                fg_product_weights_suppliers.nett_weight,
                                                fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS numeric(20,6))
      ELSE
        CAST(preliminary_inv_item.unit_price * pallet_sequences.carton_quantity AS numeric(20,6))
      END AS total_preliminary_price,

      ( SELECT string_agg( CAST(invoice_ref_no AS character varying), '; ') FROM (SELECT dn_cn.invoice_ref_no
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND dn_cn.debit_credit_type = 'Quality') AS tab) AS quality_credit_note_numbers,

      ( SELECT ABS(SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END))
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND dn_cn.debit_credit_type = 'Quality') AS quality_credit_note,

      ( SELECT ABS(SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END * pallet_sequences.carton_quantity))
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND dn_cn.debit_credit_type = 'Quality') AS total_quality_credit_note,

      ( SELECT SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END)
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND (dn_cn.debit_credit_type <> 'Quality' OR dn_cn.debit_credit_type IS NULL)) AS credit_note,

      ( SELECT SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END * pallet_sequences.carton_quantity)
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND (dn_cn.debit_credit_type <> 'Quality' OR dn_cn.debit_credit_type IS NULL)) AS total_credit_note,

      ( SELECT string_agg( debit_credit_type, '; ') FROM (SELECT dn_cn.debit_credit_type
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND (dn_cn.debit_credit_type <> 'Quality' OR dn_cn.debit_credit_type IS NULL)) AS tab) AS credit_note_type,

      ( SELECT string_agg( CAST(invoice_ref_no AS character varying), '; ') FROM (SELECT dn_cn.invoice_ref_no
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND (dn_cn.debit_credit_type <> 'Quality' OR dn_cn.debit_credit_type IS NULL)) AS tab) AS credit_note_numbers,

      ( SELECT SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END)
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id) AS debit_note,

      ( SELECT SUM(CASE WHEN dn_cn_i.price_is_per_kg THEN
                     CAST(dn_cn_i.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                        fg_product_weights_suppliers.nett_weight,
                                                        fg_product_weights.nett_weight) AS numeric(20,6))
                   ELSE
                     CAST(dn_cn_i.unit_price AS numeric(20,6))
                   END * pallet_sequences.carton_quantity)
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE'
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id) AS total_debit_note,

      ( SELECT string_agg( debit_credit_type, '; ') FROM (SELECT dn_cn.debit_credit_type
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE') AS tab) AS debit_note_type,

      ( SELECT string_agg( CAST(invoice_ref_no AS character varying), '; ') FROM (SELECT dn_cn.invoice_ref_no
          FROM invoice_items_pallet_sequences iips
          JOIN invoice_items dn_cn_i ON dn_cn_i.id = iips.invoice_item_id
          JOIN invoices dn_cn ON dn_cn.id = dn_cn_i.invoice_id
         WHERE iips.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.original_invoice_id = #{invoice_prefix}.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE') AS tab) AS debit_note_numbers,

      CASE WHEN supplier_invoice_items.price_is_per_kg THEN
        CAST(supplier_invoice_items.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                                fg_product_weights_suppliers.nett_weight,
                                                fg_product_weights.nett_weight) AS numeric(20,6))
      ELSE
        CAST(supplier_invoice_items.unit_price AS numeric(20,6))
      END AS supplier_invoiced_price_per_carton,
      CASE WHEN supplier_invoice_items.price_is_per_kg THEN
        CAST(supplier_invoice_items.unit_price * COALESCE(pallet_sequences.fg_product_nett_weight,
                                        fg_product_weights_suppliers.nett_weight,
                                        fg_product_weights.nett_weight) * pallet_sequences.carton_quantity AS numeric(20,6))
      ELSE
        CAST(supplier_invoice_items.unit_price * pallet_sequences.carton_quantity AS numeric(20,6))
      END AS total_supplier_invoice_invoiced_price,

      ( SELECT ABS(SUM(dn_cn_i.unit_price))
          FROM pallet_sequences_supplier_invoice_items pssii
          JOIN supplier_invoice_items dn_cn_i ON dn_cn_i.id = pssii.supplier_invoice_item_id
          JOIN supplier_invoices dn_cn ON dn_cn.id = dn_cn_i.supplier_invoice_id
         WHERE pssii.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_supplier_invoice_id = supplier_invoice_items.supplier_invoice_id) AS supplier_credit_note,

      ( SELECT ABS(SUM(dn_cn_i.unit_price * pallet_sequences.carton_quantity))
          FROM pallet_sequences_supplier_invoice_items pssii
          JOIN supplier_invoice_items dn_cn_i ON dn_cn_i.id = pssii.supplier_invoice_item_id
          JOIN supplier_invoices dn_cn ON dn_cn.id = dn_cn_i.supplier_invoice_id
         WHERE pssii.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'CREDIT_NOTE'
           AND dn_cn.original_supplier_invoice_id = supplier_invoice_items.supplier_invoice_id) AS total_supplier_credit_note,

      ( SELECT SUM(dn_cn_i.unit_price)
          FROM pallet_sequences_supplier_invoice_items pssii
          JOIN supplier_invoice_items dn_cn_i ON dn_cn_i.id = pssii.supplier_invoice_item_id
          JOIN supplier_invoices dn_cn ON dn_cn.id = dn_cn_i.supplier_invoice_id
         WHERE pssii.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE'
           AND dn_cn.original_supplier_invoice_id = supplier_invoice_items.supplier_invoice_id) AS supplier_debit_note,

      ( SELECT SUM(dn_cn_i.unit_price * pallet_sequences.carton_quantity)
          FROM pallet_sequences_supplier_invoice_items pssii
          JOIN supplier_invoice_items dn_cn_i ON dn_cn_i.id = pssii.supplier_invoice_item_id
          JOIN supplier_invoices dn_cn ON dn_cn.id = dn_cn_i.supplier_invoice_id
         WHERE pssii.pallet_sequence_id = pallet_sequences.id
           AND dn_cn.invoice_type = 'DEBIT_NOTE'
           AND dn_cn.original_supplier_invoice_id = supplier_invoice_items.supplier_invoice_id) AS total_supplier_debit_note,

      -- =======================================
      -- TOTAL RECEIVED - hectic calculation!
      -- (total_inv_rec + total_dn/cn_rec) / (total_invoice price / item_total_price) / total_cartons on item * cartons on pseq.
      CASE #{item_prefix}.unit_price WHEN 0 THEN 0 ELSE
      (
      COALESCE((SELECT SUM(rec.amount)
       from customer_receipt_items rec
       where rec.invoice_id = pallet_sequences.invoice_id
      ), 0.0)
      +
      COALESCE((SELECT SUM(rec.amount)
       from customer_receipt_items rec
       join invoices inv on inv.id = rec.invoice_id
       where inv.original_invoice_id = pallet_sequences.invoice_id
      ), 0.0)
      )
      / (
      COALESCE(NULLIF((SELECT SUM(CASE WHEN inv.price_is_per_kg THEN
                NULLIF(CAST(item.unit_price * item.mass_in_kg AS numeric(20,6)),0)
              ELSE
                NULLIF(CAST(item.unit_price * item.no_cartons AS numeric(20,6)),0)
              END)
       from invoice_items item
       join invoices inv on inv.id = item.invoice_id
       where item.invoice_id = pallet_sequences.invoice_id
      ), 0), 1.0)
      /
      COALESCE(NULLIF((SELECT SUM(CASE WHEN inv.price_is_per_kg THEN
                NULLIF(CAST(item.unit_price * item.mass_in_kg AS numeric(20,6)),0)
              ELSE
                NULLIF(CAST(item.unit_price * item.no_cartons AS numeric(20,6)),0)
              END)
       from invoice_items item
       join invoices inv on inv.id = item.invoice_id
       where item.id = pallet_sequences.invoice_item_id
      ), 0 ), 1.0)
      )
      /
      COALESCE((SELECT item.no_cartons
       from invoice_items item
       where item.id = pallet_sequences.invoice_item_id
      ), 1.0)

      * pallet_sequences.carton_quantity
      END
      AS total_received,
      -- =======================================

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_FREIGHT}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS freight,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_OSEAS}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS overseas_costs,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_OSEAS}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) * pallet_sequences.carton_quantity AS total_overseas_costs,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_FOB}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS actual_fob_costs_per_carton_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_PPECB}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS ppecb_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_CGA}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS cga_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_LEVIES}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS other_levies_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_PACK}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS packing_costs_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_CS}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS cold_storage_cost_per_carton_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_TPORT}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS transport_to_port_per_carton_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_PH_HAND}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS ph_handling,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_PH_DGREEN}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS ph_degreening,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_PH_PCOSTS}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS ph_packing_costs,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_GRP_REBATE}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS rebate_zar,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND cost_groups.reporting_group = '#{CostGroup::RPT_PH_CSTORE}'
         AND NOT service_provider_invoice_item_allocations.for_own_cost) AS ph_cold_storage,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND service_provider_invoice_item_allocations.is_local_cost
         AND service_provider_invoice_item_allocations.calculate_vat
         AND NOT cost_groups.ph_cost) AS vatable_costs,

      (SELECT SUM(cost_per_carton)
       FROM supplier_pallet_sequence_costs
       JOIN service_provider_invoice_item_allocations ON service_provider_invoice_item_allocations.id = supplier_pallet_sequence_costs.service_provider_invoice_item_allocation_id
       JOIN cost_codes ON cost_codes.id = service_provider_invoice_item_allocations.cost_code_id
       JOIN cost_groups ON cost_groups.id = cost_codes.cost_group_id
       WHERE supplier_pallet_sequence_costs.pallet_sequence_id = pallet_sequences.id
         AND service_provider_invoice_item_allocations.is_local_cost
         AND service_provider_invoice_item_allocations.calculate_vat
         AND cost_groups.ph_cost) AS ph_vatable_costs,

      supplier_account_sales.approved AS account_sale_finalised,
      voyage_liquidations.foreign_costs AS liquidated_foreign_costs,
      voyage_liquidations.local_costs AS liquidated_local_costs,
      voyage_liquidations.default_costs AS liquidated_default_costs,
      /*
      CASE WHEN supplier_super_groups.summarise_supplier_account_sale THEN
        vl_super.supplier_account_sales
      ELSE
        voyage_liquidation_suppliers.supplier_account_sales
      END AS liquidated_account_sale,
      */
      supplier_account_sales.completed AS account_sale_completed,
      customer_account_sale_liquidations.status AS customer_account_sale_status,
      customer_account_sale_liquidations.date_finalised AS date_cas_finalised,
      CASE WHEN customer_account_sales.requires_qc_approval THEN
        CASE WHEN customer_account_sales.qc_approved THEN
          'QC COMPLETE'
        ELSE
          'AWAITING QC'
        END
      ELSE
        NULL
      END AS customer_account_sale_qc_status,
      customer_account_sales.final_status AS customer_account_sale_final_status,
      customer_account_sales.approved_on AS customer_account_sale_approved_on,

      (SELECT SUM(sase.est_cost_per_pallet)
        FROM supplier_account_sale_estimates sase
        WHERE sase.supplier_account_sale_id = pallet_sequences.supplier_account_sale_id
          AND sase.active) AS accumulated_est_cost,

      supplier_invoices.invoice_ref_no AS supplier_invoice_ref,
      supplier_invoices.internal_ref_no AS supp_inv_internal_ref,
      COALESCE(( SELECT sum(payreqps.cash_amount) FROM supplier_payment_request_pallet_amounts payreqps
          JOIN supplier_payment_requests payreq ON payreq.id = payreqps.supplier_payment_request_id
          WHERE payreqps.pallet_sequence_id = pallet_sequences.id
            AND payreq.paid), 0.0) AS amount_paid,
      ( SELECT supplier_payment_due_dates.due_date
             FROM supplier_payment_due_dates
            WHERE supplier_payment_due_dates.supplier_invoice_id = supplier_invoices.id
            ORDER BY supplier_payment_due_dates.due_date
           LIMIT 1) AS first_pm_due_date,
      ( SELECT supplier_payment_due_dates.due_date
             FROM supplier_payment_due_dates
            WHERE supplier_payment_due_dates.supplier_invoice_id = supplier_invoices.id
            ORDER BY supplier_payment_due_dates.due_date
           OFFSET 1
           LIMIT 1) AS second_pm_due_date,
      ( SELECT supplier_payment_due_dates.due_date
             FROM supplier_payment_due_dates
            WHERE supplier_payment_due_dates.supplier_invoice_id = supplier_invoices.id
            ORDER BY supplier_payment_due_dates.due_date
           OFFSET 2
           LIMIT 1) AS final_pm_due_date,
      COALESCE(market_finalisations.status,'PENDING') as market_finalisation_status,
      market_finalisations.provision_commercial_credit,
      market_finalisations.provision_commercial_credit * pallet_sequences.carton_quantity AS total_provision_commercial_credit,
      #{invoice_prefix}.invoice_date AS customer_invoice_date,
      pool_target_markets.name AS pool_target_market,countries.country_name AS dest_country,
      CASE WHEN invoicing_parties.invoice_ref_prefix IN ('UB','UM') THEN
            'INVOICED BY FARMING ENTITIES'
           ELSE
            CASE WHEN ports.country_code IN ('ZA') THEN
                CASE WHEN item_pack_products.grade_code IN ('1','2') THEN
                      'LOCAL VIA SW EXPORT GRADE'
                     ELSE  'LOCAL VIA SW'
                END
            ELSE 'EXPORT'
           END
      END AS channel,
      CASE WHEN customers.related_party THEN 'RELATED PARTY'
        ELSE 'NON-RELATED PARTY'
      END AS related_party,
      customers.customer_type,
      suppliers.fin_supplier_type,
      commodities.reporting_commodity,
      marks.brand_type,
      CASE WHEN customer_account_sales.approved THEN 'FINALISED'
        ELSE 'UNFINALISED'
      END AS cas_finalization_status,
      pallet_sequences.production_run_id,
      first_notice_decay_condition,
      first_notice_progressive_condition,
      first_notice_non_progressive_condition,
      decay_factor_perc,
      progressive_factor_perc,
      non_progressive_factor_perc_1,
      non_progressive_factor_perc_2,
      qc_reports.notes,
      cust_org_groups.org_group_code AS customer_org_group_code,
      final_receiver_org_groups.org_group_code as final_receiver_org_group_code

      FROM pallet_sequences
      JOIN pallets ON pallets.id = pallet_sequences.pallet_id
      #{invoice_joins}
      JOIN customers ON customers.id = #{invoice_prefix}.customer_id
      LEFT OUTER JOIN customer_gl_codes ON customer_gl_codes.customer_id = customers.id AND customer_gl_codes.currency_id = #{invoice_prefix}.currency_id
      JOIN currencies ON currencies.id = #{invoice_prefix}.currency_id
      JOIN exchange_rates exchange_rate_est ON exchange_rate_est.id = #{invoice_prefix}.estimated_exchange_rate_id
      JOIN exchange_rates exchange_rate_actual ON exchange_rate_actual.id = #{invoice_prefix}.exchange_rate_id
      JOIN parties_roles cust ON cust.id = customers.parties_role_id
      LEFT OUTER JOIN parties_roles supp ON supp.id = pallet_sequences.fin_supplier_party_role_id
      JOIN deal_types ON deal_types.id = #{invoice_prefix}.deal_type_id
      JOIN inco_terms ON inco_terms.id = #{invoice_prefix}.inco_term_id
      JOIN voyages ON voyages.id = #{invoice_prefix}.voyage_id
      JOIN fg_products ON fg_products.id = pallet_sequences.fg_product_id
      JOIN item_pack_products ON item_pack_products.id = fg_products.item_pack_product_id
      JOIN varieties ON varieties.id = item_pack_products.variety_id
      LEFT OUTER JOIN variety_groups ON variety_groups.id = varieties.variety_group_id
      JOIN commodities ON commodities.id = varieties.commodity_id
      JOIN commodity_groups ON commodity_groups.id = commodities.commodity_group_id
      JOIN packs on packs.pack_code = fg_products.pack_code
      LEFT OUTER JOIN pucs ON pucs.puc_code = pallet_sequences.puc
      JOIN fg_product_weights on fg_product_weights.pack_id = packs.id and fg_product_weights.commodity_id = varieties.commodity_id
      LEFT OUTER JOIN suppliers ON suppliers.supplier_parties_role_id = pallet_sequences.fin_supplier_party_role_id
      LEFT OUTER JOIN fg_product_weights_suppliers ON fg_product_weights_suppliers.supplier_id = suppliers.id
       AND fg_product_weights_suppliers.pack_id = packs.id
       AND fg_product_weights_suppliers.commodity_id = varieties.commodity_id
      LEFT OUTER JOIN supplier_account_sales ON supplier_account_sales.id = pallet_sequences.supplier_account_sale_id
      LEFT OUTER JOIN pools ON pools.id = pallet_sequences.pool_id
      LEFT OUTER JOIN inco_terms supp_inco_terms ON supp_inco_terms.id = supplier_account_sales.inco_term_id
      LEFT OUTER JOIN supplier_groups ON supplier_groups.id = pallet_sequences.supplier_group_id
      LEFT OUTER JOIN supplier_super_groups ON supplier_super_groups.id = supplier_groups.supplier_super_group_id
      LEFT JOIN load_instruction_details d ON pallets.load_instruction_detail_id = d.id
      LEFT JOIN load_instruction_containers u ON pallets.load_instruction_container_id = u.id
      LEFT JOIN consignment_order_load_instructions ON COALESCE(u.consignment_order_load_instruction_id, d.consignment_order_load_instruction_id) = consignment_order_load_instructions.id
      LEFT JOIN consignment_orders ON consignment_order_load_instructions.consignment_order_id = consignment_orders.id
      LEFT JOIN consignments ON consignment_orders.consignment_id = consignments.id
      LEFT JOIN containers l ON u.container_id = l.id AND consignments.id = l.consignment_id
      LEFT JOIN voyage_ports j ON consignments.pod_voyage_port_id = j.id
      LEFT OUTER JOIN load_instructions ON load_instructions.id = consignment_order_load_instructions.load_instruction_id
      LEFT JOIN vessel_bookings ON l.vessel_booking_id = vessel_bookings.id
      LEFT JOIN vessel_bookings pol_vessel_bookings ON pol_vessel_bookings.id = (SELECT MAX(id) FROM vessel_bookings WHERE vessel_bookings.pol_voyage_port_id = load_instructions.pol_voyage_port_id)
      LEFT JOIN parties_roles parties_sl ON parties_sl.id = COALESCE(vessel_bookings.shipping_line_party_role_id, pol_vessel_bookings.shipping_line_party_role_id)
      LEFT OUTER JOIN standard_count_conversions std ON std.variety_id = item_pack_products.variety_id
           AND std.pack_code = fg_products.pack_code
           AND std.size_count_code = item_pack_products.size_count_code
      LEFT OUTER JOIN voyage_liquidations ON voyage_liquidations.voyage_id = #{invoice_prefix}.voyage_id
      LEFT OUTER JOIN voyage_liquidation_suppliers ON voyage_liquidation_suppliers.voyage_liquidation_id = voyage_liquidations.id
           AND voyage_liquidation_suppliers.supplier_id = suppliers.id
      LEFT OUTER JOIN suppliers super_supplier ON super_supplier.represents_supplier_super_group_id = supplier_super_groups.id
      LEFT OUTER JOIN voyage_liquidation_suppliers vl_super ON vl_super.voyage_liquidation_id = voyage_liquidations.id
           AND vl_super.supplier_id = super_supplier.id
      LEFT OUTER JOIN marketing_desks ON marketing_desks.id = #{invoice_prefix}.marketing_desk_id
      LEFT OUTER JOIN target_market_groups ON target_market_groups.target_market_group_code = #{invoice_prefix}.marketing_tm_group
           AND target_market_groups.target_market_group_type_code = 'SALES'
      LEFT OUTER JOIN target_market_groups marketing_tm ON marketing_tm.target_market_group_code = pallet_sequences.marketing_tm_group
           AND marketing_tm.target_market_group_type_code = 'MARKETING'
      LEFT OUTER JOIN target_market_groups shipping_tm ON shipping_tm.target_market_group_code = pallet_sequences.shipping_tm_group
           AND shipping_tm.target_market_group_type_code = 'SHIPPING'
      LEFT OUTER JOIN pool_finance_varieties ON pool_finance_varieties.variety_group_id = variety_groups.id
      AND pool_finance_varieties.marketing_tm_group_id = marketing_tm.id
      AND season_year = voyages.year
      LEFT OUTER JOIN pool_target_markets ON pool_target_markets.id = pool_finance_varieties.pool_target_market_id

      LEFT OUTER JOIN supplier_invoice_items ON supplier_invoice_items.id = pallet_sequences.supplier_invoice_item_id
      LEFT OUTER JOIN supplier_invoices ON supplier_invoices.id = supplier_invoice_items.supplier_invoice_id
      LEFT OUTER JOIN currencies supp_currencies ON supp_currencies.id = supplier_invoices.currency_id
      LEFT OUTER JOIN customer_account_sale_liquidations customer_account_sale_liquidations ON customer_account_sale_liquidations.invoice_id = #{invoice_prefix}.id
                                                                   AND customer_account_sale_liquidations.variety_id = item_pack_products.variety_id
      LEFT OUTER JOIN customer_account_sales ON customer_account_sales.id = pallet_sequences.customer_account_sale_id
      LEFT OUTER JOIN qc_pallet_sequences ON qc_pallet_sequences.pallet_sequence_id = pallet_sequences.id
      LEFT OUTER JOIN qc_reports ON qc_reports.id = qc_pallet_sequences.qc_report_id

      INNER JOIN psize ON psize.id = pallet_sequences.id
      LEFT OUTER JOIN market_finalisations on market_finalisations.id = pallet_sequences.market_finalisation_id

      LEFT OUTER JOIN cities on consignments.final_destination = cities.city_code
      LEFT OUTER JOIN countries on countries.id = cities.country_id

      LEFT OUTER JOIN marks on marks.mark_code = pallet_sequences.mark_code
      LEFT OUTER JOIN invoicing_parties ON invoicing_parties.parties_role_id = #{invoice_prefix}.invoicing_party_role_id
      LEFT OUTER JOIN ports ON ports.id = j.port_id AND ports.port_name::text = #{invoice_prefix}.port_of_discharge::text

      LEFT JOIN organizations cust_orgs on cust_orgs.party_id = cust.party_id
      LEFT JOIN org_groups cust_org_groups on  cust_orgs.org_group_id = cust_org_groups.id
      LEFT JOIN org_group_types cust_org_group_types on cust_org_groups.org_group_type_id = cust_org_group_types.id

      LEFT JOIN parties_roles final_receiver_parties_roles ON pallets.final_receiver_party_role_id = final_receiver_parties_roles.id
      LEFT JOIN organizations final_receiver_orgs on final_receiver_orgs.party_id = final_receiver_parties_roles.party_id
      LEFT JOIN org_groups final_receiver_org_groups on final_receiver_orgs.org_group_id = final_receiver_org_groups.id
      LEFT JOIN org_group_types final_receiver_org_group_types on final_receiver_org_groups.org_group_type_id = final_receiver_org_group_types.id

        WHERE #{extra_where} (pallets.exit_ref IS NULL OR upper(pallets.exit_ref::text) <> 'SCRAPPED'::text) AND pallet_sequences.pallet_number::text IS NOT NULL AND pallet_sequences.carton_quantity > 0 AND voyages.year = #{@voyage_year}

      -- LIMIT 2600
    SQL
  end

  # --------------------------------------------------------------------------
  # FIELDS AND THEIR TYPES
  # --------------------------------------------------------------------------

  INT_FIELDS       = %i[pallet_sequence_number preliminary_ref_no
                        total_cartons pallet_sequence_id
                        supp_accsale_id year invoice_id production_run_id].freeze
  DATE_FIELDS      = %i[eta etd atd ata customer_account_sale_approved_on
                        date_cas_finalised account_sale_due_date
                        first_pm_due_date second_pm_due_date final_pm_due_date].freeze
  DATE_TIME_FIELDS = %i[packed_date_time].freeze
  BOOL_FIELDS      = %i[account_sale_finalised liquidated_foreign_costs
                        liquidated_local_costs liquidated_default_costs
                        account_sale_completed invoice_paid_in_full pay_in_forex].freeze

  NUM_FIELDS  = %i[nett_weight total_weight nett_masterfile_weight total_masterfile_weight
                   perc pallet_size usd_etd_roe roe_on_etd roe_on_atd roe_on_acc_sale
                   roe_on_receipt invoiced_price_per_kg invoiced_price_per_carton
                   total_invoiced_price expected_invoiced_price_per_kg expected_invoiced_price_per_carton
                   total_expected_invoiced_price preliminary_price_per_kg
                   preliminary_price_per_carton total_preliminary_price
                   quality_credit_note
                   total_quality_credit_note credit_note
                   total_credit_note debit_note
                   total_debit_note packing_costs_zar
                   freight overseas_costs ppecb_zar cga_zar other_levies_zar
                   total_overseas_costs actual_fob_costs_per_carton_zar
                   supplier_invoiced_price_per_carton total_supplier_invoice_invoiced_price
                   supplier_credit_note total_supplier_credit_note
                   supplier_debit_note total_supplier_debit_note total_received
                   commercial_weight total_commercial_weight amount_paid
                   cold_storage_cost_per_carton_zar transport_to_port_per_carton_zar
                   ph_handlingph_degreeningph_packing_costsph_cold_storage rebate_zar
                   provision_commercial_credit total_provision_commercial_creditzar_total_sales
                   decay_factor_perc progressive_factor_perc non_progressive_factor_perc_1 non_progressive_factor_perc_2
                   usd_sales total_usd_sales standard_cartons
                   total_provision_commercial_credit total_usd_invoiced].freeze

  STR_FIELDS  = %i[invoice_ref_no ucr_number customer_code gl_code
                   final_receiver month shipping_week deal_category
                   deal_type receiver_inco_term supplier_inco_term
                   voyage_code voyage_short_code container_number
                   shipping_line pol pod supplier_super_group
                   supplier_group supplier qc_status financial_qc_status packing_week
                   intake_week marketing_desk customer_account_sale_status
                   pool farm_code puc farm_within_farm pallet_number
                   season_code commodity_group_description commodity_code
                   variety_code variety_group size_count_code
                   mark_code inventory_code grade_code pack_code shipping_tm_group
                   sale_tm fin_tm profit_commission currency_code
                   quality_credit_note_numbers credit_note_type
                   credit_note_numbers debit_note_type debit_note_numbers
                   orchard phc seal_point customer_account_sale_qc_status
                   customer_account_sale_final_status marketing_tm_description
                   supplier_invoice_ref supp_inv_internal_ref supplier_currency
                   market_finalisation_status variety_description pool_target_market low_chem packed_tm_group dest_country
                   channel related_party customer_type fin_supplier_type reporting_commodity brand_type cas_finalization_status
                   first_notice_decay_condition first_notice_progressive_condition first_notice_non_progressive_condition notes
                   customer_org_group_code final_receiver_org_group_code].freeze

  FIELD_DEFS = {
    ucr_number: :string,
    customer_code: :string,
    gl_code: :string,
    final_receiver: :string,
    month: :string,
    year: :integer,
    shipping_week: :string,
    deal_category: :string,
    deal_type: :string,
    receiver_inco_term: :string,
    supplier_inco_term: :string,
    voyage_code: :string,
    voyage_short_code: :string,
    container_number: :string,
    shipping_line: :string,
    pol: :string,
    pod: :string,
    eta: :date,
    etd: :date,
    ata: :date,
    supplier_super_group: :string,
    supplier_group: :string,
    supplier: :string,
    qc_status: :string,
    financial_qc_status: :string,
    packing_week: :string,
    pool: :string,
    farm_code: :string,
    pallet_number: :string,
    season_code: :string,
    commodity_code: :string,
    variety_code: :string,
    size_count_code: :string,
    mark_code: :string,
    grade_code: :string,
    nett_weight: :numeric,
    total_weight: :numeric,
    nett_masterfile_weight: :numeric,
    total_masterfile_weight: :numeric,
    pack_code: :string,
    shipping_tm_group: :string,
    sale_tm: :string,
    fin_tm: :string,
    pool_target_market: :string,
    profit_commission: :string,
    perc: :numeric,
    pallet_size: :numeric,
    total_cartons: :integer,
    standard_cartons: :numeric,
    currency_code: :string,
    usd_etd_roe: :numeric,
    roe_on_etd: :numeric,
    roe_on_acc_sale: :numeric,
    roe_on_receipt: :numeric,
    invoiced_price_per_kg: :numeric,
    invoiced_price_per_carton: :numeric,
    total_invoiced_price: :numeric,
    expected_invoiced_price_per_kg: :numeric,
    expected_invoiced_price_per_carton: :numeric,
    total_expected_invoiced_price: :numeric,
    quality_credit_note_numbers: :string,
    quality_credit_note: :numeric,
    total_quality_credit_note: :numeric,
    quality_credit_note_zar: :numeric,
    total_quality_credit_note_zar: :numeric,
    credit_note: :numeric,
    total_credit_note: :numeric,
    credit_note_type: :string,
    credit_note_numbers: :string,
    debit_note: :numeric,
    total_debit_note: :numeric,
    debit_note_type: :string,
    debit_note_numbers: :string,
    total_received: :numeric,
    cif_fob: :numeric,
    total_cif_fob: :numeric,
    freight: :numeric,
    total_freight: :numeric,
    overseas_costs: :numeric,
    total_overseas_costs: :numeric,
    pre_profit_fob: :numeric,
    total_pre_profit_fob: :numeric,
    profit: :numeric,
    total_profit: :numeric,
    post_profit_fob: :numeric,
    total_post_profit_fob: :numeric,
    commission: :numeric,
    total_commission: :numeric,
    supplier_fob: :numeric,
    total_supplier_fob: :numeric,
    estimated_fob_costs_per_crtn: :numeric,
    total_estimated_fob_costs: :numeric,
    actual_fob_costs_per_ctn: :numeric,
    total_actual_fob_costs: :numeric,
    dip: :numeric,
    total_dip: :numeric,
    pre_profit_fob_zar: :numeric,
    total_pre_profit_fob_zar: :numeric,
    profit_zar: :numeric,
    total_profit_zar: :numeric,
    post_profit_fob_zar: :numeric,
    total_post_profit_fob_zar: :numeric,
    commission_zar: :numeric,
    total_commission_zar: :numeric,
    supplier_fob_zar: :numeric,
    total_supplier_fob_zar: :numeric,
    estimated_fob_cost_per_carton_zar: :numeric,
    total_estimated_fob_costs_zar: :numeric,
    actual_fob_costs_per_carton_zar: :numeric,
    total_actual_fob_costs_zar: :numeric,
    dip_zar: :numeric,
    total_dip_zar: :numeric,
    ppecb_zar: :numeric,
    total_ppecb_zar: :numeric,
    cga_zar: :numeric,
    total_cga_zar: :numeric,
    other_levies_zar: :numeric,
    total_other_zar: :numeric,
    ppecb: :numeric,
    total_ppecb: :numeric,
    cga: :numeric,
    total_cga: :numeric,
    other_levies: :numeric,
    total_other: :numeric,
    vat_zar: :numeric,
    total_vat_zar: :numeric,
    due_to_supplier_per_carton_zar: :numeric,
    total_due_to_supplier_zar: :numeric,
    account_sale_finalised: :boolean,
    puc: :string,
    preliminary_ref_no: :integer,
    preliminary_price_per_kg: :numeric,
    preliminary_price_per_carton: :numeric,
    total_preliminary_price: :numeric,
    pallet_sequence_id: :integer,
    pallet_number_adjusted: :string,
    intake_week: :string,
    packed_date_time: :time,
    pallet_sequence_number: :integer,
    commodity_group_description: :string,
    liquidated_foreign_costs: :boolean,
    liquidated_local_costs: :boolean,
    liquidated_default_costs: :boolean,
    account_sale_completed: :boolean,
    marketing_desk: :string,
    roe_on_atd: :numeric,
    supp_accsale_id: :integer,
    inventory_code: :string,
    invoice_ref_no: :string,
    atd: :date,
    farm_within_farm: :string,
    variety_group: :string,
    variety_description: :string,
    supplier_invoiced_price_per_carton: :numeric,
    total_supplier_invoice_invoiced_price: :numeric,
    supplier_credit_note: :numeric,
    total_supplier_credit_note: :numeric,
    supplier_debit_note: :numeric,
    total_supplier_debit_note: :numeric,
    packing_costs_zar: :numeric,
    total_packing_costs_zar: :numeric,
    packing_costs: :numeric,
    total_packing_costs: :numeric,
    vat: :numeric,
    total_vat: :numeric,
    due_to_supplier_per_carton: :numeric,
    total_due_to_supplier: :numeric,
    customer_account_sale_status: :string,
    orchard: :string,
    phc: :string,
    seal_point: :string,
    customer_account_sale_qc_status: :string,
    customer_account_sale_final_status: :string,
    customer_account_sale_approved_on: :date,
    commercial_weight: :numeric,
    total_commercial_weight: :numeric,
    date_cas_finalised: :date,
    invoice_paid_in_full: :boolean,
    supplier_invoice_ref: :string,
    supp_inv_internal_ref: :string,
    marketing_tm_description: :string,
    pay_in_forex: :boolean,
    supplier_currency: :string,
    account_sale_due_date: :date,
    amount_paid: :numeric,
    first_pm_due_date: :date,
    second_pm_due_date: :date,
    final_pm_due_date: :date,
    ex_cold_store_per_carton_zar: :numeric,
    total_ex_cold_store_zar: :numeric,
    cold_storage_cost_per_carton_zar: :numeric,
    total_cold_storage_costs_zar: :numeric,
    transport_to_port_per_carton_zar: :numeric,
    total_transport_to_port_zar: :numeric,
    ex_cold_store_per_carton: :numeric,
    total_ex_cold_store: :numeric,
    cold_storage_cost_per_carton: :numeric,
    total_cold_storage_costs: :numeric,
    transport_to_port_per_carton: :numeric,
    total_transport_to_port: :numeric,
    market_finalisation_status: :string,
    provision_commercial_credit: :numeric,
    total_provision_commercial_credit: :numeric,
    ph_handling: :numeric,
    total_ph_handling: :numeric,
    ph_degreening: :numeric,
    total_ph_degreening: :numeric,
    ph_packing_costs: :numeric,
    total_ph_packing_costs: :numeric,
    rebate: :numeric,
    total_rebate: :numeric,
    rebate_zar: :numeric,
    total_rebate_zar: :numeric,
    ph_cold_storage: :numeric,
    total_ph_cold_storage: :numeric,
    ph_vat: :numeric,
    total_ph_vat: :numeric,
    ph_supplier_nett: :numeric,
    total_ph_supplier_nett: :numeric,
    low_chem: :string,
    packed_tm_group: :string,
    dest_country: :string,
    invoice_id: :integer,
    channel: :string,
    related_party: :string,
    customer_type: :string,
    fin_supplier_type: :string,
    reporting_commodity: :string,
    brand_type: :string,
    zar_total_sales: :numeric,
    cas_finalization_status: :string,
    production_run_id: :integer,
    first_notice_decay_condition: :string,
    first_notice_progressive_condition: :string,
    first_notice_non_progressive_condition: :string,
    decay_factor_perc: :numeric,
    progressive_factor_perc: :numeric,
    non_progressive_factor_perc_1: :numeric,
    non_progressive_factor_perc_2: :numeric,
    notes: :string,
    customer_org_group_code: :string,
    final_receiver_org_group_code: :string,
    usd_sales: :numeric,
    total_usd_sales: :numeric,
    total_usd_invoiced: :numeric
  }.freeze
  COL_NAMES = FIELD_DEFS.keys
  NUM_COLS = FIELD_DEFS.select { |_, v| v == :numeric }.map { |k, _| k }

  # Get the column names from an SQL statement.
  def list_of_cols_from_stat(stat)
    # Grab everything between SELECT and the last FROM. NB. This will fail if there is a subquery in the WHERE clause or in a JOIN.
    # Regex is modified by m for multiline and i for case insensitive.
    match = stat.match(/\A\s*select\s?(.+)(\sfrom\s)/mi)
    # Need to strip FUNCTION(field,1,2) out, but NOT COUNT(id)
    # If there is a match, replace any functions that may include commas, then split columns by commas and return an array of the last word in each column.
    # Function regex:
    # \w+        Start with words (SUBSTR)
    # \s?        Might be a space or tab or two between the function name and parenthesis
    # \(         Match an open parenthesis
    # [^\)]+?    The ( can be followed by 1 or more of any character except ")"
    # ,{1}       There must be exactly one ","
    # .+?        ...followed by one or more of any character
    # \)         ...and ending with a closing parenthesis
    match.nil? ? nil : match[1].gsub(/\w+\s?\([^\)]+?,{1}.+?\)/, 'HIDEFUNC').split(',').map { |c| c.split('.').last.split(' ').last }
  end
end

# EXPORTER_CLIENT=unifrutti script/runner -e production 'require "lib/tasks/export_full_season_with_copy"; ExportFullSeasonWithCopy.make_spreadsheet(2016)'
# EXPORTER_CLIENT=unifrutti script/runner -e production 'require "lib/tasks/export_full_season_with_copy"; ExportFullSeasonWithCopy.upload_spreadsheet(2016)'
# EXPORTER_CLIENT=unifrutti script/runner -e production 'require "lib/tasks/export_full_season_with_copy"; ExportFullSeasonWithCopy.re_populate_table(2016)'
# EXPORTER_CLIENT=unifrutti script/runner -e production 'require "lib/tasks/export_full_season_with_copy"; ExportFullSeasonWithCopy.show_query_sql'

ExportFullSeasonWithCopy.upload_spreadsheet(year.to_i)
# puts ExportFullSeasonWithCopy.show_query_sql

# rubocop:enable Layout/LineLength
# rubocop:enable Naming/VariableNumber
# rubocop:enable Metrics/MethodLength
# rubocop:enable Metrics/ClassLength
# rubocop:enable Style/OptionalBooleanParameter
