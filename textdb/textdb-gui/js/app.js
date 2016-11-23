var main = function(){
		
    $('.icon-menu').click(function(){
        $('.menu').animate({
            'left': '0px'
        }, 200);
		
		$('.icon-menu').css({
			'visibility': 'hidden',
			'pointer': 'default'
		});
		
        $('body').animate({
            'left': '285px'
        }, 200);
    });
	
    $('.icon-close').click(function(){
        $('.menu').animate({
            'left': '-285px'
        }, 200);
		
		$('.icon-menu').css({
			'visibility': 'visible',
			'pointer': 'pointer'
		});
		
        $('body').animate({
            'left': '0px'
        }, 200);
    });
};

$(document).ready(main);
