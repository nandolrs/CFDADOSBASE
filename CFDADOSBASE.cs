using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

//using Oracle.DataAccess.Client;
//using Oracle.DataAccess.Types;

using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Data;
using System.Data.SqlClient;
using System.Collections;

using System.ComponentModel.DataAnnotations.Schema;

namespace cf.dados
{
    public class ATabela : Attribute
    {
        string _nome;
        string _sequencia;

        ATabelaColuna[] _colunas;

        public ATabela()
        {
        }

        public string nome
        {
            get { return _nome; }
            set { _nome = value; }
        }

        public string sequencia
        {
            get { return _sequencia; }
            set { _sequencia = value; }
        }

        public ATabelaColuna[] colunas
        {
            get { return _colunas; }
            set { _colunas = value; }
        }
    }

    public class ATabelaColuna : Attribute
    {
        string _nome;
        string _tipo;
        object _valor;
        bool _chave;
        bool _chaveEstrangeira;

        public ATabelaColuna()
        {
        }

        public string nome
        {
            get { return _nome; }
            set { _nome = value; }
        }

        public string tipo
        {
            get { return _tipo; }
            set { _tipo = value; }
        }

        public object valor
        {
            get { return _valor; }
            set { _valor = value; }
        }

        public bool chave
        {
            get { return _chave; }
            set { _chave = value; }
        }

        public bool chaveEstrangeira
        {
            get { return _chaveEstrangeira; }
            set { _chaveEstrangeira = value; }
        }
    }

    public class EntidadeBase : IEnumerator, IEnumerable
    {
        protected Oracle.ManagedDataAccess.Client.OracleConnection oOracleConnection;
        protected Oracle.ManagedDataAccess.Client.OracleCommand oOracleCommand;
        protected Oracle.ManagedDataAccess.Client.OracleDataAdapter oOracleDataAdapter;
        protected System.Data.DataSet oDataSet;
        protected System.Data.DataSet _dataSet;
        protected int _posicao=-1;
        protected Int64 _codigo;
        protected long _Count;

        protected string _sqlPesquisar;
        protected string _sqlPesquisarColunas;
        protected string _sqlPesquisarWhere;

        protected string _sqlIncluir;
        protected string _sqlIncluirColunas = "";
        protected string _sqlIncluirValores = "";

        protected string _sqlAlterar;
        protected string _sqlAlterarColunas = "";
        protected string _sqlAlterarValores = "";
        protected string _sqlAlterarWhere = "";

        protected string _sqlConsultar;
        protected string _sqlConsultarColunas = "";
        protected string _sqlConsultarValores = "";
        protected string _sqlConsultarWhere = "";

        public EntidadeBase[] buscaEntidades()
        {
            return buscaEntidades(this);
        }

        public EntidadeBase[] buscaEntidades(EntidadeBase entidade)
        {
            cf.dados.EntidadeBase[] entidades = { };

            Array.Resize(ref entidades, entidades.Length + 1);
            entidades[entidades.Length - 1] = entidade;

            foreach (System.Reflection.PropertyInfo oPropertyInfo in entidade.GetType().GetProperties())
            {
                object oValor = oPropertyInfo.GetValue(entidade);

                Boolean isBase = (oValor is EntidadeBase);

                if (isBase || (oPropertyInfo.PropertyType.BaseType != null && oPropertyInfo.PropertyType.BaseType.Name == "EntidadeBase"))
                {
                    Array.Resize(ref entidades, entidades.Length + 1);
                    entidades[entidades.Length - 1] = (cf.dados.EntidadeBase)oValor;
                }

            }

            return entidades;
        }

        public virtual Int64 codigo
        {
            get
            {
                return _codigo;
            }

            set
            {
                _codigo = value;
            }
        }

        protected void conectando()
        {
            string sServidor = "XE";// --HOMOLOG
            string sUsuario = "homolog1";
            string sUsuarioSenha = "homolog1";

            sServidor = System.Configuration.ConfigurationManager.AppSettings["credenciais.servidor"];
            sUsuario = System.Configuration.ConfigurationManager.AppSettings["credenciais.usuario"];
            sUsuarioSenha = System.Configuration.ConfigurationManager.AppSettings["credenciais.senha"];

            //sServidor = oBiblioteca.configLer("credenciais.servidor");
            //sUsuario = oBiblioteca.configLer("credenciais.usuario");
            //sUsuarioSenha = oBiblioteca.configLer("credenciais.senha");



            string sConexao = "User id=" + sUsuario + ";Password=" + sUsuarioSenha + ";Data Source=" + sServidor;

            oOracleConnection = new OracleConnection(sConexao);
            oOracleConnection.Open();
        }

        protected void desconectando()
        {
            oOracleConnection.Clone();
        }

        protected ATabela montandoTabela()
        {

            ATabela oATabela = null;
            ATabelaColuna oATabelaColuna = null;

            // verifica se é ATabela
            bool bATabela = false;

            object[] oAtributosTabela = this.GetType().GetCustomAttributes(typeof(ATabela), true);
            foreach (object obj in oAtributosTabela)
            {
                bATabela = (obj is ATabela);
                if (bATabela) { oATabela = (ATabela)obj; break; }
            }

            if (!bATabela) { throw new Exception("objeto deve ser do tipo ATabela"); }

            // busca as propriedades

            ATabelaColuna[] colunas = { };

            foreach (System.Reflection.PropertyInfo oPropertyInfo in this.GetType().GetProperties())
            {
                // busca as colunas

                object[] oAtributosColuna = oPropertyInfo.GetCustomAttributes(typeof(ATabelaColuna), true);

                bool bTabelaColuna = false;
                foreach (object obj in oAtributosColuna)
                {
                    bTabelaColuna = (obj is ATabelaColuna);
                    if (bTabelaColuna) { oATabelaColuna = (ATabelaColuna)obj; break; }
                }

                if (bTabelaColuna) // é coluna
                {
                    object objValor = oPropertyInfo.GetValue(this);
                    oATabelaColuna.valor = objValor;

                    string sTabelaNome = oATabela.nome;
                    string sTabelaColuna = oATabelaColuna.nome;
                    string sTabelaColunaTipo = oATabelaColuna.tipo;

                    Array.Resize(ref colunas, colunas.Length + 1);
                    colunas[colunas.Length - 1] = oATabelaColuna;
                }
                else // pode ser tabela
                {

                    ATabela oATabelaRelacionada = null;

                    oAtributosTabela = oPropertyInfo.GetCustomAttributes(typeof(ATabela), true);
                    if (oAtributosTabela != null && oAtributosTabela.Length > 0)
                    {
                        // é tabela, busca o valor da propriedade e busca a coluna chave

                        oATabelaRelacionada = (ATabela)oAtributosTabela[0];

                        object objValorTabela = oPropertyInfo.GetValue(this);

                        foreach (System.Reflection.PropertyInfo oPropertyInfo1 in objValorTabela.GetType().GetProperties())
                        {
                            oAtributosColuna = oPropertyInfo1.GetCustomAttributes(typeof(ATabelaColuna), true);
                            if (oAtributosColuna != null && oAtributosColuna.Length > 0)
                            {
                                oATabelaColuna = (ATabelaColuna)oAtributosColuna[0];
                                if (oATabelaColuna.chave)
                                {
                                    oATabelaColuna.chave = false;
                                    oATabelaColuna.chaveEstrangeira = true;

                                    object objValor = oPropertyInfo1.GetValue(objValorTabela);
                                    oATabelaColuna.valor = objValor;

                                    string sTabelaNome = oATabelaRelacionada.nome;
                                    string sTabelaColuna = oATabelaColuna.nome;
                                    string sTabelaColunaTipo = oATabelaColuna.tipo;

                                    Array.Resize(ref colunas, colunas.Length + 1);
                                    colunas[colunas.Length - 1] = oATabelaColuna;
                                    break;

                                }

                            }

                        }

                    }

                }

            }

            oATabela.colunas = colunas;

            return oATabela;
        }

        protected System.Data.DataSet consultando(string sql)
        {
            string sSql = sql;

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);

            oDataSet = new System.Data.DataSet();

            oOracleDataAdapter.Fill(oDataSet);
            _posicao = -1;

            this.desconectando();

            return oDataSet;

        }

        public int sequencia()
        {
            ATabela oTabela = montandoTabela();

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(null, oOracleConnection);

            // monta sql

            string sSql = "select <TABELA_SEQUENCIA_NOME>.nextval as proximoValor from dual";

            sSql = sSql.Replace("<TABELA_SEQUENCIA_NOME>", oTabela.sequencia != null ? oTabela.sequencia : oTabela.nome + "_SEQ");

            System.Data.DataSet oDataSet = this.consultando(sSql);

            string sRetorno = oDataSet.Tables[0].Rows[0]["proximoValor"].ToString();
            return int.Parse(sRetorno);

        }

        protected IEnumerable _lista;

        //public delegate void EV_consultando();

        //public event EV_consultando evConsultando;

        public delegate void EV_propriedando();

        public event EV_propriedando evPropriedando;

        public void propriedar(cf.comum.enumeracoes.CFACAO acao)
        {
            propriedandoInicio(acao);

            int contadorEntidade = 0;

            foreach (EntidadeBase entidadeBase in this.buscaEntidades())
            {
                contadorEntidade++;

                foreach (System.Reflection.PropertyInfo oPropertyInfo in entidadeBase.GetType().GetProperties())
                {
                    bool bChave = false;
                    bool bIgnorar = false;
                    object[] atributos = oPropertyInfo.GetCustomAttributes(true);
                    ATabelaColuna aTabelaColuna = null;

                    foreach (object atributo in atributos)
                    {
                        if (atributo is System.ComponentModel.DataAnnotations.KeyAttribute)
                        {
                            bChave = true;
                        }
                        if (atributo is System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute)
                        {
                            bIgnorar = true;
                        }
                        if (atributo is ATabelaColuna)
                        {
                            aTabelaColuna = (ATabelaColuna)atributo;
                            if (!bChave) { bChave = aTabelaColuna.chave; }
                        }

                    }
                    if (bIgnorar) { continue; }
                    if (contadorEntidade > 1 && !bChave) { continue; }

                    object valor = oPropertyInfo.GetValue(entidadeBase);

                    propriedando(acao, oPropertyInfo.Name, oPropertyInfo.PropertyType, (object)entidadeBase, bChave, valor, aTabelaColuna);

                    if (acao == comum.enumeracoes.CFACAO.carregando)
                    {
                        bool bTiposDesejados = "Int64xDecimalxStringxDateTime".IndexOf(oPropertyInfo.PropertyType.Name) != -1;
                        if (bTiposDesejados)
                        {
                            string sNome = (aTabelaColuna.nome != null ? aTabelaColuna.nome : oPropertyInfo.Name);

                            if (_posicao < 0 && _dataSet.Tables[0].Rows.Count > 0)
                            {
                                _posicao = 0;
                            }

                            object valorColuna = _dataSet.Tables[0].Rows[_posicao][sNome];

                            if ((valorColuna == null) || (valorColuna is DBNull))
                            {
                                oPropertyInfo.SetValue(entidadeBase, null);
                                continue;
                            }


                            if (oPropertyInfo.PropertyType.Name == "Int64")
                            {
                                oPropertyInfo.SetValue(entidadeBase, Int64.Parse(valorColuna.ToString()));
                            }
                            else if (oPropertyInfo.PropertyType.Name == "Decimal")
                            {
                                oPropertyInfo.SetValue(entidadeBase, decimal.Parse(valorColuna.ToString()));
                            }
                            else if (oPropertyInfo.PropertyType.Name == "String")
                            {
                                oPropertyInfo.SetValue(entidadeBase, valorColuna.ToString());
                            }
                            else if (oPropertyInfo.PropertyType.Name == "DateTime")
                            {
                                oPropertyInfo.SetValue(entidadeBase, DateTime.Parse(valorColuna.ToString()));

                            }

                        }

                    }


                } // foreach (System.Reflection.PropertyInfo oPropertyInfo in entidadeBase.GetType().GetProperties())
            } // foreach (EntidadeBase entidadeBase in this.buscaEntidades())



            propriedandoFinal(acao);
        }


        public virtual void propriedandoInicio(cf.comum.enumeracoes.CFACAO acao)
        {

        }

        public virtual void propriedandoFinal(cf.comum.enumeracoes.CFACAO acao)
        {

        }

        public virtual void propriedando(cf.comum.enumeracoes.CFACAO acao, string nome, Type tipo, object o, bool chave, object valor, ATabelaColuna aTabelaColuna)
        {


        }

        [NotMapped]
        public IEnumerable lista
        {
            get { return _lista; }
        }

        public virtual void consultar(bool filhos)
        {
            _dataSet = new DataSet();

            if (this.codigo == 0) { throw new cf.dados.erros.EntidadeChaveNaoInformada(); }
            propriedar(comum.enumeracoes.CFACAO.consultando);
            sqlComando(_sqlConsultar, _dataSet);
            _Count = _dataSet.Tables[0].Rows.Count;
            carregar(filhos);

        }
        public virtual void consultar()
        {
            _dataSet = new DataSet();

            if (this.codigo == 0) { throw new cf.dados.erros.EntidadeChaveNaoInformada(); }
            propriedar(comum.enumeracoes.CFACAO.consultando);
            sqlComando(_sqlConsultar, _dataSet);
            _Count = _dataSet.Tables[0].Rows.Count;
            carregar();

        }

        public virtual void consultar1()
        {
            _dataSet = new DataSet();

            if (this.codigo == 0) { throw new cf.dados.erros.EntidadeChaveNaoInformada(); }
            propriedar(comum.enumeracoes.CFACAO.consultando);
            sqlComando(_sqlConsultar, _dataSet);
            _Count = _dataSet.Tables[0].Rows.Count;
            carregar1();

        }

        public virtual void alterar()
        {
            if (this.codigo == 0) { throw new cf.dados.erros.EntidadeChaveNaoInformada(); }
            propriedar(comum.enumeracoes.CFACAO.alterando);
            sqlComando(_sqlAlterar);
        }

        public void incluir()
        {
            this.codigo = this.sequencia();
            propriedar(comum.enumeracoes.CFACAO.incluindo);
            sqlComando(_sqlIncluir);
        }

        public virtual void pesquisar()
        {
            _dataSet = new DataSet();

            propriedar(comum.enumeracoes.CFACAO.pesquisando);
            sqlComando(_sqlPesquisar, _dataSet);
            _lista =this;

        }
        protected void carregar(bool filhos)
        {
            propriedar(comum.enumeracoes.CFACAO.carregando);
            if (filhos) { carregarFilhos(); }
        }

        protected void carregar()
        {
            propriedar(comum.enumeracoes.CFACAO.carregando);
        }

        protected void carregar1()
        {
            propriedar(comum.enumeracoes.CFACAO.carregando);
        }

        protected void carregarFilhos()
        {
            int i = -1;
            foreach (EntidadeBase entidadeBase in buscaEntidades(this))
            {
                i++;
                if (i > 0)
                {
                    long codigo = entidadeBase.codigo;
                    entidadeBase.consultar1();
                }

            }
        }
        protected virtual void sqlComando(string sql)
        {
        }

        protected virtual void sqlComando(string sql, DataSet dataSet)
        {
        }


        public virtual void excluir()
        {
        }


        string pesquisarMontaSQL()
        {
            propriedar(comum.enumeracoes.CFACAO.pesquisando);

            return _sqlPesquisar;
        }



        [NotMapped]
        public long Count
        {
            get { return _Count; }
            set { _Count = value; }
        }

        protected void carregarLista()
        {
            //ATabelaColuna /*oATabelaColuna*/ = null;

            // busca as propriedades

            object oLista = null;

            foreach (object objLista in _lista)
            {
                oLista = objLista;
            }

            foreach (System.Reflection.PropertyInfo oPropertyInfo in this.GetType().GetProperties())
            {
                object[] oAtributosColuna = oPropertyInfo.GetCustomAttributes(true);


                foreach (object o in oAtributosColuna)
                {
                    if (o is System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute) { continue; }
                }
                // System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute,

                //if (oAtributosColuna.Length > 0) { continue; }



                //if (oAtributosColuna != null && oAtributosColuna.Length > 0) // bTabelaColuna
                //{
                // é uma coluna

                ///*oATabelaColuna*/ = (ATabelaColuna)oAtributosColuna[0];

                foreach (System.Reflection.PropertyInfo oPropertyInfo1 in oLista.GetType().GetProperties())
                {
                    if (oPropertyInfo.Name == oPropertyInfo1.Name)
                    {
                        object objValorLista = oPropertyInfo1.GetValue(oLista);

                        string sTipo = oPropertyInfo.PropertyType.Name;

                        bool bCarregar = "Int64xDateTimexDecimalxString".IndexOf(sTipo) > -1;

                        if (!(objValorLista is DBNull) && bCarregar)
                        {
                            if (oPropertyInfo.PropertyType.Name == "Int64")
                            {
                                oPropertyInfo.SetValue(this, Int64.Parse(objValorLista.ToString()));
                            }
                            else
                            {
                                oPropertyInfo.SetValue(this, objValorLista);
                            }
                            break;
                        }
                    }
                }
                //}//
            }
        }

        // marca:inicio:lista

        IEnumerator IEnumerable.GetEnumerator()
        {
            try
            {
                object oRow = _dataSet.Tables[_posicao < 0 ? 0 : _posicao];
                return this;
            }

            catch (IndexOutOfRangeException)
            {
                throw new InvalidOperationException();
            }

        }

        bool IEnumerator.MoveNext()
        {
            _posicao++;
            return _posicao < _dataSet.Tables[0].Rows.Count;
        }

        void IEnumerator.Reset()
        {
            _posicao = -1;
        }

        object IEnumerator.Current
        {
            get
            {
                this.carregar(false); 

                //if (evConsultando != null)
                //{
                //    evConsultando();
                //}

                return this;
            }
        }
        // marca:fim:lista
    }


    public interface iEntidade
    {
        Int64 codigo { get; set; }

        void incluir();
        void alterar();
        void excluir();
        void consultar();
        void pesquisar();
    }

    public class EntidadeLista : EntidadeBase, IEnumerable ,  IEnumerator
    {
        public EntidadeLista()
        {
        }


        IEnumerator IEnumerable.GetEnumerator()
        {
            try
            {
                object oRow = _dataSet.Tables[_posicao < 0 ? 0 : _posicao];
                return this;
            }

            catch (IndexOutOfRangeException)
            {
                throw new InvalidOperationException();
            }

        }

        bool IEnumerator.MoveNext()
        {
            _posicao++;
            return _posicao < _dataSet.Tables[0].Rows.Count;
        }

        void IEnumerator.Reset()
        {
            _posicao = -1;
        }

        object IEnumerator.Current
        {
            get
            {
                this.carregar();

                //if (evConsultando != null)
                //{
                //    evConsultando();
                //}

                return this;
            }
        }
    }

    public class Entidade : EntidadeBase, iEntidade, System.Collections.IEnumerable, IEnumerator
    {
        cf.util.Biblioteca oBiblioteca;

        int _Count;

        protected event EV_consultando evConsultando;
        protected delegate void EV_consultando();

        public int Count
        {
            get { return _Count; }
            set { _Count = value; }
        }





        public Entidade() { _codigo = 0; }

        void conectar()
        {
            conectando();
            oBiblioteca = new util.Biblioteca();
        }

        public void alterar()
        {
            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(null, oOracleConnection);

            //

            ATabela oTabela = montandoTabela();

            string sSql = " UPDATE <TABELA_NOME> SET <COLUNAS> WHERE <CONDICAO>";

            sSql = sSql.Replace("<TABELA_NOME>", oTabela.nome);

            string sSqlColunas = "";
            string sSqlCondicao = "";

            foreach (ATabelaColuna oColuna in oTabela.colunas)
            {
                if (oColuna.valor != null) // se tiver valor a ser atualizado
                {
                    if (oColuna.chave)
                    {
                        sSqlCondicao += (sSqlCondicao != "" ? " AND " : "");
                        sSqlCondicao += oColuna.nome + " = " + oColuna.valor;
                    }
                    else // (oColuna.chave)
                    {
                        sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome + " = ";

                        if (oColuna.tipo == "string")
                        {
                            sSqlColunas += "'" + oColuna.valor.ToString() + "'";
                        }
                        else if (oColuna.tipo == "Int64")
                        {
                            //sSqlColunas += oColuna.valor.ToString();
                            if (oColuna.valor.ToString().Length == 0 || Int64.Parse(oColuna.valor.ToString()) == 0)
                            {
                                sSqlColunas += "null";
                            }
                            else
                            {
                                sSqlColunas += oColuna.valor.ToString();
                            }
                        }
                        else if (oColuna.tipo == "decimal")
                        {
                            sSqlColunas += oColuna.valor.ToString();
                        }
                        else if (oColuna.tipo == "DateTime")
                        {
                            string sValor = " TO_DATE('" + DateTime.Parse(oColuna.valor.ToString()).ToString("yyyy/MM/dd HH:mm:ss") + "','yyyy/mm/dd hh24:mi:ss')";
                            sSqlColunas += sValor;
                        }
                        else if (oColuna.tipo == "BLOB")
                        {
                            // cria o parâmetro do tipo blob

                            string sParametroNome = "p" + oColuna.nome;
                            OracleParameter oParametro = new OracleParameter();
                            oParametro.OracleDbType = OracleDbType.Blob;
                            oParametro.ParameterName = sParametroNome;

                            System.IO.Stream oStream = (System.IO.Stream)oColuna.valor;
                            byte[] bValor = new byte[(int)oStream.Length];

                            oStream.Read(bValor, 0, (int)oStream.Length);

                            oParametro.Value = bValor;

                            oOracleCommand.Parameters.Add(oParametro);

                            //adiciona a coluna na instrução sql

                            string sValor = ":" + sParametroNome;

                            sSqlColunas += sValor;

                        }

                    }
                } // (oColuna.valor != null)  

            }

            sSql = sSql.Replace("<COLUNAS>", sSqlColunas);
            sSql = sSql.Replace("<CONDICAO>", sSqlCondicao);

            // executa  o sql

            oOracleCommand.CommandText = sSql;
            oOracleCommand.Prepare();
            oOracleCommand.ExecuteNonQuery();

            this.desconectando();
        }

        void consultar(bool dependencia)
        {
            if (!dependencia && this.codigo == 0) { throw new Exception("Informe o código."); }

            ATabela oTabela = montandoTabela();

            string sSql = " SELECT <COLUNAS> FROM <TABELA_NOME>  WHERE <CONDICAO>";

            sSql = sSql.Replace("<TABELA_NOME>", oTabela.nome);

            string sSqlColunas = "";
            string sSqlCondicao = "";

            foreach (ATabelaColuna oColuna in oTabela.colunas)
            {
                sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome;

                if (oColuna.chave)
                {
                    sSqlCondicao += (sSqlCondicao != "" ? " AND " : "");
                    sSqlCondicao += oColuna.nome + " = " + oColuna.valor;
                }
                //else
                //{
                //    sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome;
                //}
            }

            sSql = sSql.Replace("<COLUNAS>", sSqlColunas);
            sSql = sSql.Replace("<CONDICAO>", sSqlCondicao);

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);
            string s = oOracleCommand.CommandText;

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);
            oDataSet = new System.Data.DataSet();
            oOracleDataAdapter.Fill(oDataSet);

            this.desconectando();

            _posicao = -1;
            _Count = oDataSet.Tables[0].Rows.Count;

            oTabela = carregar(oDataSet, oTabela);

            carregar(oTabela);

            if (evConsultando != null)
            {
                evConsultando();
            }

        }

        protected void consultar(string sql)
        {
            ATabela oTabela = montandoTabela();

            string sSql = sql;

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);
            string s = oOracleCommand.CommandText;

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);
            oDataSet = new System.Data.DataSet();
            oOracleDataAdapter.Fill(oDataSet);

            this.desconectando();

            _posicao = -1;
            _Count = oDataSet.Tables[0].Rows.Count;

            oTabela = carregar(oDataSet, oTabela);

            carregar(oTabela);

            if (evConsultando != null)
            {
                evConsultando();
            }

        }

        public void consultarDependencia()
        {
            bool dependencia = true;
            consultar(dependencia);
        }

        public void consultar()
        {
            bool dependencia = false;
            consultar(dependencia);
            carregar();
        }

        protected ATabela carregar(System.Data.DataSet dataSet, ATabela tabela) // 1 de 2
        {
            foreach (ATabelaColuna oColuna in tabela.colunas)
            {
                if (_Count > 0)// (!oColuna.chave)
                {
                    oColuna.valor = dataSet.Tables[0].Rows[_posicao == -1 ? 0 : _posicao][oColuna.nome];
                }
            }

            return tabela;
        }

        protected void carregar(ATabela tabela) // 2 de 2 tabela para objeto
        {
            ATabelaColuna oATabelaColuna = null;

            // busca as propriedades

            foreach (System.Reflection.PropertyInfo oPropertyInfo in this.GetType().GetProperties())
            {
                object[] oAtributosColuna = oPropertyInfo.GetCustomAttributes(typeof(ATabelaColuna), true);


                if (oAtributosColuna != null && oAtributosColuna.Length > 0) // bTabelaColuna
                {
                    // é uma coluna

                    oATabelaColuna = (ATabelaColuna)oAtributosColuna[0];

                    foreach (ATabelaColuna coluna in tabela.colunas)
                    {
                        if (oATabelaColuna.nome == coluna.nome)
                        {
                            if (!(coluna.valor is DBNull))
                            {
                                if (coluna.tipo == "Int64")
                                {
                                    oPropertyInfo.SetValue(this, Int64.Parse(coluna.valor.ToString()));
                                }
                                else
                                {
                                    oPropertyInfo.SetValue(this, coluna.valor);
                                }
                                break;
                            }
                        }
                    }
                }
                //else // pode ser um relacionamento (tabela)
                //{
                //    oAtributosColuna = oPropertyInfo.GetCustomAttributes(typeof(ATabela), true);

                //    if (oAtributosColuna != null && oAtributosColuna.Length > 0)
                //    {
                //        é uma tabela

                //    object objValorTabela = oPropertyInfo.GetValue(this);

                //        foreach (System.Reflection.PropertyInfo oPropertyInfo1 in objValorTabela.GetType().GetProperties())
                //        {
                //            oAtributosColuna = oPropertyInfo1.GetCustomAttributes(typeof(ATabelaColuna), true);

                //            if (oAtributosColuna != null && oAtributosColuna.Length > 0) // bTabelaColuna
                //            {
                //                // é uma coluna

                //                oATabelaColuna = (ATabelaColuna)oAtributosColuna[0];
                //                if (oATabelaColuna.chave)
                //                {
                //                    foreach (ATabelaColuna coluna in tabela.colunas)
                //                    {
                //                        if (oATabelaColuna.nome == coluna.nome)
                //                        {
                //                            if (!(coluna.valor is DBNull))
                //                            {
                //                                if (coluna.tipo == "Int64")
                //                                {
                //                                    oPropertyInfo1.SetValue(objValorTabela, Int64.Parse(coluna.valor.ToString()));
                //                                }
                //                                else
                //                                {
                //                                    oPropertyInfo1.SetValue(objValorTabela, coluna.valor);
                //                                }

                //                            }
                //                            break;

                //                        }
                //                    }
                //                }

                //            }
                //        }



                //    } //
                //}
            }
        }

        public void excluir()
        {
            iEntidade oEntidade = (iEntidade)this;
            if (oEntidade.codigo == 0) { throw new Exception("Chave não informada."); }

            ATabela oTabela = montandoTabela();

            string sSql = " DELETE <TABELA_NOME> WHERE <CONDICAO>";

            sSql = sSql.Replace("<TABELA_NOME>", oTabela.nome);

            string sSqlCondicao = "";

            foreach (ATabelaColuna oColuna in oTabela.colunas)
            {
                if (oColuna.chave)
                {
                    sSqlCondicao += (sSqlCondicao != "" ? " AND " : "");
                    sSqlCondicao += oColuna.nome + " = " + oColuna.valor;
                }
            }

            sSql = sSql.Replace("<CONDICAO>", sSqlCondicao);

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);
            string s = oOracleCommand.CommandText;
            oOracleCommand.ExecuteNonQuery();

            this.desconectando();
        }

        public void incluir()
        {
            ATabela oTabela = montandoTabela();

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }
            oOracleCommand = new OracleCommand(null, oOracleConnection);

            // monta sql

            string sSql = " INSERT INTO <TABELA_NOME> (<COLUNAS>) VALUES (<VALORES>)";

            sSql = sSql.Replace("<TABELA_NOME>", oTabela.nome);

            string sSqlColunas = "";
            string sSqlValores = "";

            foreach (ATabelaColuna oColuna in oTabela.colunas)
            {

                if (oColuna.valor != null || (oColuna.tipo == "DateTime" && (DateTime)oColuna.valor != DateTime.MinValue))
                {

                    sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome;
                    sSqlValores += (sSqlValores != "" ? "," : "");


                    if (oColuna.chave)
                    {
                        sSqlValores += oTabela.nome + "_SEQ.nextval";
                    }
                    else
                    {
                        if (oColuna.tipo == "string")
                        {
                            sSqlValores += "'" + oColuna.valor.ToString() + "'";
                        }
                        else if (oColuna.tipo == "Int64")
                        {
                            if (oColuna.valor.ToString().Length == 0 || Int64.Parse(oColuna.valor.ToString()) == 0)
                            {
                                sSqlValores += "null";
                            }
                            else
                            {
                                sSqlValores += oColuna.valor.ToString();
                            }
                        }
                        else if (oColuna.tipo == "decimal")
                        {
                            sSqlValores += oColuna.valor.ToString();
                        }
                        else if (oColuna.tipo == "DateTime")
                        {
                            string sValor = " TO_DATE('" + DateTime.Parse(oColuna.valor.ToString()).ToString("yyyy/MM/dd HH:mm:ss") + "','yyyy/mm/dd hh24:mi:ss')";
                            sSqlValores += sValor;
                        }
                        else if (oColuna.tipo == "BLOB")
                        {
                            // cria o parâmetro do tipo blob

                            string sParametroNome = "p" + oColuna.nome;
                            OracleParameter oParametro = new OracleParameter();
                            oParametro.OracleDbType = OracleDbType.Blob;
                            oParametro.ParameterName = sParametroNome;

                            System.IO.Stream oStream = (System.IO.Stream)oColuna.valor;
                            byte[] bValor = new byte[(int)oStream.Length];

                            oStream.Read(bValor, 0, (int)oStream.Length);


                            oParametro.Value = bValor;

                            oOracleCommand.Parameters.Add(oParametro);

                            //adiciona a coluna na instrução sql

                            string sValor = ":" + sParametroNome;
                            sSqlValores += sValor;
                        }
                    }

                }
                else
                {
                    sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome;
                    sSqlValores += (sSqlValores != "" ? "," : "");

                    sSqlValores += "null";
                }

            }

            sSql = sSql.Replace("<COLUNAS>", sSqlColunas);
            sSql = sSql.Replace("<VALORES>", sSqlValores);

            // executa  o sql

            oOracleCommand.CommandText = sSql;
            oOracleCommand.Prepare();
            oOracleCommand.ExecuteNonQuery();

            this.desconectando();
        }

        public void pesquisar()
        {
            ATabela oTabela = montandoTabela();

            string sSql = " SELECT <COLUNAS> FROM <TABELA_NOME>  "; //  WHERE <CONDICAO>

            sSql = sSql.Replace("<TABELA_NOME>", oTabela.nome);

            string sSqlColunas = "";
            string sSqlCondicao = "";

            foreach (ATabelaColuna oColuna in oTabela.colunas)
            {
                sSqlColunas += (sSqlColunas != "" ? "," : "") + oColuna.nome;

                if (oColuna.valor != null)
                {

                    if (oColuna.tipo == "Int64")
                    {
                        if (((Int64)oColuna.valor) != 0)
                        {
                            sSqlCondicao += (sSqlCondicao != "" ? " AND " : "");
                            sSqlCondicao += oColuna.nome + " = " + oColuna.valor;

                        }
                    }
                    else if (oColuna.tipo == "string")
                    {
                        if (((string)oColuna.valor) != "")
                        {
                            sSqlCondicao += (sSqlCondicao != "" ? " AND " : "");
                            sSqlCondicao += oColuna.nome + " = '" + oColuna.valor + "'";

                        }
                    }
                }
            }

            sSql = sSql.Replace("<COLUNAS>", sSqlColunas);
            if (sSqlCondicao != "")
            {
                sSql += "WHERE " + sSqlCondicao;

            }

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);
            string s = oOracleCommand.CommandText;

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);
            oDataSet = new System.Data.DataSet();
            oOracleDataAdapter.Fill(oDataSet);

            this.desconectando();

            _posicao = -1;
            _Count = oDataSet.Tables[0].Rows.Count;

            if (_Count > 0)
            {
                oTabela = carregar(oDataSet, oTabela);

                carregar(oTabela);
            }


        }

        protected void pesquisar(string sql)
        {
            ATabela oTabela = montandoTabela();

            string sSql = sql;

            // conecta com sgdb

            if (oOracleConnection == null)
            {
                this.conectando();
            }

            oOracleCommand = new OracleCommand(sSql, oOracleConnection);
            string s = oOracleCommand.CommandText;

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);
            oDataSet = new System.Data.DataSet();
            oOracleDataAdapter.Fill(oDataSet);

            this.desconectando();

            _posicao = -1;
            _Count = oDataSet.Tables[0].Rows.Count;

            if (_Count > 0)
            {
                oTabela = carregar(oDataSet, oTabela);

                carregar(oTabela);
            }


        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            try
            {
                object oRow = oDataSet.Tables[_posicao < 0 ? 0 : _posicao];
                return this;
            }

            catch (IndexOutOfRangeException)
            {
                throw new InvalidOperationException();
            }

        }

        bool IEnumerator.MoveNext()
        {
            _posicao++;
            return _posicao < oDataSet.Tables[0].Rows.Count;
        }

        void IEnumerator.Reset()
        {
            _posicao = -1;
        }

        object IEnumerator.Current
        {
            get
            {
                ATabela oTabela = montandoTabela();
                oTabela = carregar(oDataSet, oTabela);

                carregar(oTabela);

                if (evConsultando != null)
                {
                    evConsultando();
                }

                return this;
            }
        }
    }

    public class EntidadeOracle : EntidadeBase,  iEntidade
    {
        public  EV_consultando evConsultando;
        public delegate void EV_consultando();

        public override void propriedandoInicio(cf.comum.enumeracoes.CFACAO acao)
        {
            if (acao == comum.enumeracoes.CFACAO.pesquisando)
            {
                _sqlPesquisar = "";
                _sqlPesquisarColunas = "";
                _sqlPesquisarWhere = "";
            }
            else if (acao == comum.enumeracoes.CFACAO.incluindo)
            {
                _sqlIncluir = "";
                _sqlIncluirColunas = "";
                _sqlIncluirValores = "";
            }
            else if (acao == comum.enumeracoes.CFACAO.alterando)
            {
                _sqlAlterar = "";
                _sqlAlterarColunas = "";
                _sqlAlterarWhere = "";
            }
            else if (acao == comum.enumeracoes.CFACAO.consultando)
            {
                _sqlConsultar = "";
                _sqlConsultarColunas = "";
                _sqlConsultarWhere = "";
            }

        }

        public override void propriedandoFinal(cf.comum.enumeracoes.CFACAO acao)
        {
            if (acao == comum.enumeracoes.CFACAO.pesquisando)
            {
                _sqlPesquisar = " SELECT " + _sqlPesquisarColunas + " FROM " + this.GetType().Name;
                if (_sqlPesquisarWhere.Length > 0)
                {
                    _sqlPesquisar += " WHERE " + _sqlPesquisarWhere;
                }

            }
            else if (acao == comum.enumeracoes.CFACAO.incluindo)
            {
                _sqlIncluir = " INSERT INTO <TABELA_NOME> (<COLUNAS>) VALUES ( <VALORES>)";

                _sqlIncluir = _sqlIncluir.Replace("<TABELA_NOME>", this.GetType().Name);
                _sqlIncluir = _sqlIncluir.Replace("<COLUNAS>", _sqlIncluirColunas);
                _sqlIncluir = _sqlIncluir.Replace("<VALORES>", _sqlIncluirValores);
            }
            else if (acao == comum.enumeracoes.CFACAO.alterando)
            {
                _sqlAlterar = " UPDATE <TABELA_NOME> SET <COLUNAS> WHERE <COLUNAS_CONDICAO>";

                _sqlAlterar = _sqlAlterar.Replace("<TABELA_NOME>", this.GetType().Name);
                _sqlAlterar = _sqlAlterar.Replace("<COLUNAS>", _sqlAlterarColunas);
                _sqlAlterar = _sqlAlterar.Replace("<COLUNAS_CONDICAO>", _sqlAlterarWhere);
            }
            else if (acao == comum.enumeracoes.CFACAO.consultando)
            {
                _sqlConsultar = " SELECT <COLUNAS> FROM <TABELA_NOME>  WHERE <COLUNAS_CONDICAO>";

                _sqlConsultar = _sqlConsultar.Replace("<TABELA_NOME>", this.GetType().Name);
                _sqlConsultar = _sqlConsultar.Replace("<COLUNAS>", _sqlConsultarColunas);
                _sqlConsultar = _sqlConsultar.Replace("<COLUNAS_CONDICAO>", _sqlConsultarWhere);
            }
            else if (acao == comum.enumeracoes.CFACAO.carregando)
            {
                _sqlConsultar = " SELECT <COLUNAS> FROM <TABELA_NOME>  WHERE <COLUNAS_CONDICAO>";

                _sqlConsultar = _sqlConsultar.Replace("<TABELA_NOME>", this.GetType().Name);
                _sqlConsultar = _sqlConsultar.Replace("<COLUNAS>", _sqlConsultarColunas);
                _sqlConsultar = _sqlConsultar.Replace("<COLUNAS_CONDICAO>", _sqlConsultarWhere);
            }
        }


        public override void propriedando(cf.comum.enumeracoes.CFACAO acao, string nome, Type tipo, object o, bool chave, object valor, ATabelaColuna aTabelaColuna)
        {
            bool bOpcionais = tipo.FullName.IndexOf("DateTime") != -1;

            bool bTiposDesejados = "Int64xDecimalxStringxDateTime".IndexOf(tipo.Name) != -1;

            if (!bOpcionais && !bTiposDesejados) { return; }

            if (acao == comum.enumeracoes.CFACAO.pesquisando)
            {
                _sqlPesquisarColunas += (_sqlPesquisarColunas.Length > 0 ? ", " : "") + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome);

                if (tipo.Name == "Int64")
                {
                    if (valor != null && ((Int64)valor) > 0)
                    {
                        _sqlPesquisarWhere += (_sqlPesquisarWhere.Length > 0 ? " AND " : "") + "  " + nome + " = " + valor.ToString();

                    }

                }
                else if (tipo.Name == "Decimal")
                {
                    if (valor != null && ((decimal)valor) > 0)
                    {
                        _sqlPesquisarWhere += (_sqlPesquisarWhere.Length > 0 ? " AND " : "") + "  " + nome + " = " + valor.ToString();

                    }
                }
                else if (tipo.Name == "String")
                {
                    if (valor != null && valor.ToString().Length > 0)
                    {
                        _sqlPesquisarWhere += (_sqlPesquisarWhere.Length > 0 ? " AND " : "") + "  " + nome + " like '%" + valor.ToString() + "%'";

                    }
                }
                else if (tipo.Name == "DateTime")
                {
                    //if (valor != null && valor.ToString().Length > 0)
                    //{
                    //    _sqlPesquisarWhere += "  " + nome + " = '" + valor.ToString() + "'";

                    //}
                }
            }
            else if (acao == comum.enumeracoes.CFACAO.incluindo)
            {
                _sqlIncluirColunas +=( (_sqlIncluirColunas.Length > 0 ? ", " : "") + (aTabelaColuna.nome != null ? aTabelaColuna.nome: nome));

                if (tipo.Name == "Int64")
                {
                    _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + valor.ToString();
                }
                else if (tipo.Name == "Decimal")
                {
                    _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + valor.ToString().Replace(",", ".");
//                    _sqlAlterarColunas += valor.ToString().Replace(",", ".");

                }
                else if (tipo.Name == "String")
                {
                    _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", '" : "'") + valor.ToString() + "'";
                }
                else if (tipo.Name == "DateTime")
                {
                    if ("DT_CADASTROxDT_ALTERACAO".IndexOf(aTabelaColuna.nome.ToUpper()) > -1 )
                    {
                        _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + "sysdate";
                    }
                    else
                    {
                        string d = DateTime.Parse(valor.ToString()).ToString("dd-MM-yyyy");

                        if (d == "01-01-0001") { d = "null"; } else { d = " to_date('" + d + "','DD-MM-YYYY')"; }

                        _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + d;
                    }
                }



            }
            else if (acao == comum.enumeracoes.CFACAO.alterando)
            {
                if (chave)
                {
                    if (tipo.Name == "Int64")
                    {
                        if (valor != null && ((Int64)valor) > 0)
                        {
                            _sqlAlterarWhere += (_sqlAlterarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = " + valor.ToString();

                        }

                    }
                    else if (tipo.Name == "Decimal")
                    {
                        if (valor != null && ((decimal)valor) > 0)
                        {
                            _sqlAlterarWhere += (_sqlAlterarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = " + valor.ToString();

                        }
                    }
                    else if (tipo.Name == "String")
                    {
                        if (valor != null && valor.ToString().Length > 0)
                        {
                            _sqlAlterarWhere += (_sqlAlterarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = '" + valor.ToString() + "'";

                        }
                    }
                    else if (tipo.Name == "DateTime")
                    {



                        if (valor != null && valor.ToString().Length > 0)
                        {
                            _sqlAlterarWhere += (_sqlAlterarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = '" + valor.ToString() + "'";

                        }
                    }

                }
                else
                {
                    _sqlAlterarColunas += (_sqlAlterarColunas.Length > 0 ? ", " : "") + nome + " = ";

                    if (tipo.Name == "Int64")
                    {
                        if (valor != null) // && ((Int64)valor) > 0
                        {
                            _sqlAlterarColunas += valor.ToString();

                        }

                    }
                    else if (tipo.Name == "Decimal")
                    {
                        if (valor != null && ((decimal)valor) > 0)
                        {
                            //_sqlAlterarColunas += valor.ToString();
                            //_sqlAlterarColunas += ((decimal) valor).ToString("0.0000");
                            _sqlAlterarColunas += valor.ToString().Replace(",", ".");

                        }

                    }
                    else if (tipo.Name == "String")
                    {
                        if (valor != null && valor.ToString().Length > 0)
                        {
                            _sqlAlterarColunas += "'" + valor.ToString() + "'";

                        }
                    }
                    else if (tipo.Name == "DateTime")
                    {

                        if ("DT_ALTERACAO".IndexOf(aTabelaColuna.nome.ToUpper()) > -1)
                        {
                            _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + "sysdate";
                            _sqlAlterarColunas += "sysdate";
                        }
                        else
                        {
                            string d = DateTime.Parse(valor.ToString()).ToString("ddMMyyyy");

                            if (d == "01010001") { d = "null"; } else { d = " to_date('" + d + "')"; }

                            _sqlIncluirValores += (_sqlIncluirValores.Length > 0 ? ", " : "") + d;

                            _sqlAlterarColunas += d;
                        }





                    }

                }


            }
            else if (acao == comum.enumeracoes.CFACAO.consultando)
            {
                _sqlConsultarColunas += (_sqlConsultarColunas.Length > 0 ? ", " : "") + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome);

                if (chave)
                {
                    if (tipo.Name == "Int64")
                    {
                        if (valor != null && ((Int64)valor) > 0)
                        {
                            _sqlConsultarWhere += (_sqlConsultarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = " + valor.ToString();

                        }

                    }
                    else if (tipo.Name == "Decimal")
                    {
                        if (valor != null && ((decimal)valor) > 0)
                        {
                            _sqlConsultarWhere += (_sqlConsultarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = " + valor.ToString();

                        }
                    }
                    else if (tipo.Name == "String")
                    {
                        if (valor != null && valor.ToString().Length > 0)
                        {
                            _sqlConsultarWhere += (_sqlConsultarWhere.Length > 0 ? " AND " : "") + "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " like '%" + valor.ToString() + "%'";

                        }
                    }
                    else if (tipo.Name == "DateTime")
                    {
                        if (valor != null && valor.ToString().Length > 0)
                        {
                            _sqlConsultarWhere += "  " + (aTabelaColuna.nome != null ? aTabelaColuna.nome : nome) + " = '" + valor.ToString() + "'";

                        }
                    }
                }

          
            }


        }

        protected override void sqlComando(string sql)
        {
            // executa  o sql

            this.conectando();

            oOracleCommand.CommandText = sql;
            oOracleCommand.Prepare();
            oOracleCommand.ExecuteNonQuery();

            this.desconectando();
        }

        protected override void sqlComando(string sql, DataSet dataSet)
        {
            // executa  o sql

            this.conectando();

            oOracleCommand = new OracleCommand(sql, oOracleConnection);

            oOracleDataAdapter = new OracleDataAdapter(oOracleCommand);
            if (dataSet == null)
            {
                dataSet = new System.Data.DataSet();
            }

            oOracleDataAdapter.Fill(dataSet);

            this.desconectando();
        }


    }
}
