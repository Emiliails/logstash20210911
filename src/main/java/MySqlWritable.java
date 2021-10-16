import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable implements DBWritable {
    private CompanyNameCount companyNameCount;

    public MySqlWritable(CompanyNameCount companyNameCount) {
        this.companyNameCount = companyNameCount;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,companyNameCount.getCompanyName());
        preparedStatement.setInt(2,companyNameCount.getCount());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
