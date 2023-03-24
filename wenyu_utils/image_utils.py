# -*- coding:utf-8 -*-

# Name: image_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2022/7/29 15:49

from PIL import Image, ImageFont, ImageDraw
import pygame
from pygame.locals import *


def join_images(img_list, out_path, expand=1, _min=True):
    # 1、首先使用open创建Image对象，open()需要图片的路径作为参数
    # 2、然后获取size，size[0]代表宽，size[1]代表长，分别代表坐标轴的x,y
    # 3、使用Image.new创建一个新的对象
    # 4、设置地点，两个图片分别在大图的什么位置粘贴
    # 5、粘贴进大图，使用save()保存图像
    img_list = [Image.open(i) for i in img_list]
    size_list = [i.size for i in img_list]
    width_list = [s[0] for s in size_list]
    height_list = [s[1] for s in size_list]
    print(size_list)
    w, h = 0, 0
    if expand == 1:
        joint = Image.new("RGB", (min(width_list) if _min else max(width_list), sum(height_list)))
        for img in img_list:
            joint.paste(img, (w, h))
            h += img.size[1]
    else:
        joint = Image.new("RGB", (sum(width_list)), min(height_list) if _min else max(height_list))
        for img in img_list:
            joint.paste(img, (w, h))
            w += img.size[0]
    print(joint.size)
    joint.save(out_path)


def text2image(content, out_path):
    pygame.init()
    font = pygame.font.SysFont('Microsoft Yahei', 88)
    ftext = font.render(content, True, (65, 83, 130), (255, 255, 255))
    print(ftext.get_size())
    pygame.image.save(ftext, out_path)
    return out_path


data = {
    'tdate': '2022/07/28',
    'day_amt': 232.12,
    'month_amt': 2254.68,
    'month_complete': '96.08',
}
text_list = [
    " 战略运营分析                                                                                            ",
    f" {data['tdate']}复爱合缘婚恋业务运营日报                                                                  ",
    f"   当日销售额{data['day_amt']}万元                                                                       ",
    f"   当月累计销售额{data['day_amt']}万元,月达成率{data['month_complete']}                                    ",
]
image_list = ['test.jpg',]
for i in range(len(text_list)):
    image_list.append(text2image(text_list[i], f'test{i}.jpg'))
join_images(image_list, 'out.jpg')
