namespace MSSQLTOMYSQLConverter.Models
{
    public class BindingRowModel
    {
        public string TableName { get; set; }
        public string DataType { get; set; }
        public string IsNull { get; set; }
        public bool SpecialRow { get; set; }
        public bool IsFK { get; set; }
    }
}