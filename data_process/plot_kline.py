from pyecharts import Kline, online, Overlap, Scatter, Style, Line, Grid, Bar


class ShowKline(object):
    def __init__(self, stock_name, jsPath='https://cdn.bootcss.com/echarts/4.1.0.rc2'):
        # 使用最新的echarts js文件
        online(jsPath)
        # K线
        self.stock_name = stock_name
        self.kline = Kline(stock_name)
        self.volume_bar = Bar('')

    def chart_init(self, data_df, show_BI=True, show_Line=True, show_Center=True):
        # 获取K线数据
        def get_K_data(df):
            kdata = df.loc[:, ['open', 'close', 'low', 'high']].to_dict('split')['data']
            xaxis = list(df.index.tolist())
            return xaxis, kdata

        # 获取成交量数据
        def get_vol_data(df):
            vol_data = list(df['volume'].values)
            vol_axis = list(df.index.tolist())
            return vol_axis, vol_data

        kx, ky = get_K_data(data_df)
        vol_axis, vol_data = get_vol_data(data_df)
        # 添加K线，is_datazoom_show显示时间轴，datazoom_range时间轴范围
        self.kline.add('K', kx, ky, is_datazoom_show=True, datazoom_range=[80, 100])
        # 修改K线颜色
        self.kline._option['series'][0]['itemStyle'] = {
            'normal': {'color': '#ef232a', 'color0': '#14b143', 'borderColor': '#ef232a',
                       'borderColor0': '#14b143'}}
        # 添加bar图
        self.volume_bar.add('vol', vol_axis, vol_data, is_datazoom_show=True, datazoom_range=[80, 100])

    # 主图
    def get_main_chart(self, height=600, width=1000):
        overlap = Overlap(height=height, width=height)
        overlap.add(self.kline)
        return overlap

    # 辅图
    def get_vol_chart(self, height=200, width=1000):
        overlap = Overlap(height=height, width=height)
        overlap.add(self.volume_bar)
        return overlap

    # 获取图，并显示
    def show_chart(self, zoom_start=50, height=1000, width=2000):
        grid = Grid(height=height, width=width)

        main_ov = self.get_main_chart(height - 200, width)
        vol_ov = self.get_vol_chart(200, width)

        grid.add(main_ov, grid_top=0, grid_bottom=220)
        grid.add(vol_ov, grid_top=height - 200)

        grid._option['legend'][1]['show'] = False
        grid._option['color'] = ['#145b7d', '#e0861a', '#ef232a', '#14b143']
        grid.render('./{}.html'.format(self.stock_name))
        return grid
