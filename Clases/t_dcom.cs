using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_dcom
    {
        public string ordenedecompra { get; set; }
        public string producto { get; set; }
        public int cantidad { get; set; }
        public int valor { get; set; }
        public Boolean estado { get; set; }
        public DateTime fechahoramovimiento { get; set; }
        public int porcentaje { get; set; }
        public int descuento { get; set; }
        public int sugerido { get; set; }
        public int requerido { get; set; }
        public int bonificacion { get; set; }
        public string observacion { get; set; }
        public string sucursal { get; set; }
        public int diasinventario { get; set; }
        public int iva { get; set; }
        public int impuestoconsumo { get; set; }
        public string talla { get; set; }
        public string nombreproducto { get; set; }
        public string subgruposproducto { get; set; }
        public string marcasproducto { get; set; }
        public string tipoProducto { get; set; }
        public int valoriva { get; set; }
        public int valorimpoconsumo { get; set; }
        public int valordscto { get; set; }
        public string ean8 { get; set; }
        public string ean13 { get; set; }
        public string codigoreferencia { get; set; }
        public int valorbonificacion { get; set; }

       }
}
